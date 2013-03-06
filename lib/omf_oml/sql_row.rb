
require 'omf_oml/tuple'
require 'omf_oml/table'

module OMF::OML
        

  
  # Read the content of a table and feeds it out as a tuple store.
  # After creation of the object. The actual tuple feed is started
  # with a call to +run+.
  #
  class OmlSqlRow < OmlTuple
    attr_reader :row
    
    # 
    # Create a representation of a row in a database. Can be used to fill a table.
    #
    # @param [String] sql_table_name - name of SQL table in respective SQL database
    # @param [OmlSchema] schema - the schema describing the tuple
    # @param [Sequel] db - Database 
    # @param [Hash] opts:  
    #   - offset: Ignore first +offset+ rows. If negative or zero serve +offset+ rows initially
    #   - limit: Number of rows to fetch each time [1000]
    #   - check_interval: Interval in seconds when to check for new data. If 0, only run once.
    #   - query_interval: Interval between consecutive queries when processing large result set.
    #
    def initialize(sql_table_name, schema, db, opts = {})
      @sname = sql_table_name
      @schema = schema
      raise "Expected OmlSchema but got '#{schema.class}" unless schema.is_a? OmlSchema
      @db = db
      
      unless @offset = opts[:offset]
        @offset = 0
      end
      @limit = opts[:limit]
      @limit = 1000 unless @limit
      
      @check_interval = opts[:check_interval]
      @check_interval = 0 unless @check_interval
      @query_interval = opts[:query_interval]
      
      @on_new_vector_proc = {}

      super opts[:name] || sql_table_name, schema 
    end
    
    
    # Return a specific element of the vector identified either
    # by it's name, or its col index
    #
    def [](name_or_index)
      if name_or_index.is_a? Integer
        @row[@schema.name_at_index(name_or_index)]
      else
        unless @row.key? name_or_index
          raise "Unknown column name '#{name_or_index}'"
        end 
        @row[name_or_index]
      end
    end
    
    # Return the elements of the row as an array using the 
    # associated schema or 'schema' if non-nil.
    #
    def to_a(schema = nil)
      a = (schema || @schema).hash_to_row(@row) # don't need type conversion as sequel is doing this for us
    end
    
    # Return an array including the values for the names elements
    # given as parameters.
    #
    def select(*col_names)
      r = @row
      col_names.collect do |n|
        unless @row.key? n
          raise "Unknown column name '#{n}'"
        end 
        @row[n]
      end
    end
        
    def ts
      self[:oml_ts_server]
    end
    
    def seq_no
      self[:oml_seq]
    end    
    
    # Register a proc to be called when a new tuple arrived
    # on this stream.
    #
    def on_new_tuple(key = :_, &proc)
      if proc
        @on_new_vector_proc[key] = proc
      else
        @on_new_vector_proc.delete key
      end
      run() unless @on_new_vector_proc.empty?
    end

    # Create and return an +OmlTable+ which captures this tuple stream
    #
    # The argument to this method are either a list of columns to 
    # to capture in the table, or an array of column names and
    # an option hash  or just 
    # the option hash to be provided to the +OmlTable+ constructor.
    #
    # If a block is provided, any arriving tuple is executed by the block
    # which is expected to return an array which is added to the table
    # or nil in which case nothing is added. If a selector array is given the 
    # block is called with an array of values in the order of the columns
    # listed in the selector. Otherwise, the block is called directly 
    # with the tuple.
    #
    # opts:
    #   :schema - use this schema instead for the table
    #   :name   - name to use for table
    #   ....    - remaining options to be passed to table constructur
    #
    # def capture_in_table(*args, &block)
      # if args.length == 1
        # if args[0].kind_of?(Array)
          # select = args[0]
        # elsif args[0].kind_of?(Hash)
          # opts = args[0]
        # end
      # elsif args.length == 2 && args[1].kind_of?(Hash)
        # select = args[0]
        # opts = args[1]
      # else
        # opts = {}
        # select = args
      # end
#       
      # if (tschema = opts.delete(:schema))
        # # unless tschema[0].kind_of? Hash
          # # tschema = tschema.collect do |cname| {:name => cname} end
        # # end 
      # else
        # tschema = select.collect do |cname| {:name => cname} end
      # end
      # tname = opts.delete(:name) || stream_name
      # t = OMF::OML::OmlTable.new(tname, tschema, opts)
      # if block
        # self.on_new_tuple() do |v|
          # #puts "New vector(#{tname}): #{v.schema.inspect} ---- #{v.select(*select).size} <#{v.select(*select).join('|')}>"
          # if select
            # row = block.call(v.select(*select))
          # else
            # row = block.call(v)
          # end             
          # if row
            # raise "Expected kind of Array, but got '#{row.inspect}'" unless row.kind_of?(Array)
            # t.add_row(row)
          # end  
        # end
      # else
        # self.on_new_tuple() do |v|
          # #puts "New vector(#{tname}): #{v.select(*select).join('|')}"
          # t.add_row(v.select(*select))   
        # end
      # end
      # t
    # end
    
    
    # Return a table which will capture the content of this tuple stream.
    #
    # @param [string] name - Name to use for returned table
    # @param [Hash] opts Options to be passed on to Table constructor
    # @opts [boolean] opts :include_oml_internals If true will also include OML header columns
    # @opts [OmlSchema] opts :schema use specific schema for table (Needs to be a subset of the tuples schema)
    def to_table(name = nil, opts = {})
      unless name
        name = @sname
      end
      unless schema = opts.delete(:schema)
        include_oml_internals = (opts[:include_oml_internals] != false)
        schema = self.schema.clone(!include_oml_internals)
        if include_oml_internals
          # replace sender_id by sender ... see _run_once
          schema.replace_column_at 0, :oml_sender
        end
      end
      t = OMF::OML::OmlTable.new(name, schema, opts)
      #puts ">>>>SCHEMA>>> #{schema.inspect}"
      self.on_new_tuple() do |v|
        r = v.to_a(schema)
        #puts r.inspect
        t.add_row(r)   
      end
      t
    end


    protected
        
    # def parse_schema(raw)
      # #puts ">> PARSE SCHEMA"
      # sd = raw.collect do |col| 
        # name, opts = col
        # #puts col.inspect
        # {:name => name, :type => opts[:db_type]}
      # end
      # # Query we are using is adding the 'oml_sender_name' to the front of the table
      # sd.insert(0, :name => :oml_sender, :type => :string)
      # OmlSchema.new(sd)
    # end
#     
    # # override
    # def process_schema(schema)
      # # i = 0
      # # @vprocs = {}
      # # schema.each_column do |col|
        # # name = col[:name]
        # # j = i + 0
        # # l = @vprocs[name] = lambda do |r| r[j] end
        # # @vprocs[i - 4] = l if i > 4
        # # i += 1
      # # end
    # end
    
    def run(in_thread = true)
      return if @running
      if in_thread
        Thread.new do
          begin
            _run
          rescue Exception => ex
            error "Exception in OmlSqlRow: #{ex}"
            debug "Exception in OmlSqlRow: #{ex.backtrace.join("\n\t")}"
          end
        end
      else
        _run
      end
    end
    
    private
    
    def _run
      if @check_interval <= 0
        while _run_once; end
      else
        @running = true
        while (@running)
          begin 
            unless _run_once
              # All rows read, wait a bit for news to show up
              sleep @check_interval
            end
          rescue Exception => ex
            warn ex
            debug "\t", ex.backtrace.join("\n\t")
          end
        end 
      end
    end
      
    # Run a query on database an serve all rows found one at a time.
    # Return true if there might be more rows in the database
    def _run_once
      row_cnt = 0
      t = table_name = @sname
      if (@offset < 0)
        cnt = @db[table_name.to_sym].count()
        @offset = cnt + @offset # @offset was negative here
        debug("Initial offset #{@offset} in '#{table_name}' with #{cnt} rows")
        @offset = 0 if @offset < 0
      end
      @db["SELECT _senders.name as oml_sender, #{t}.* FROM #{t} INNER JOIN _senders ON (_senders.id = #{t}.oml_sender_id) LIMIT #{@limit} OFFSET #{@offset};"].each do |r|
      #@db["SELECT _senders.name as oml_sender, #{t}.* FROM #{t} JOIN _senders WHERE #{t}.oml_sender_id = _senders.id LIMIT #{@limit} OFFSET #{@offset};"].each do |r|
        #puts "ROW #{t}>>> #{r.inspect}"
        @row = r
        @on_new_vector_proc.each_value do |proc|
          proc.call(self)
        end
        row_cnt += 1
      end
      @offset += row_cnt
      debug "Read #{row_cnt} (total #{@offset}) rows from '#{@sname}'" if row_cnt > 0
      if more_to_read = row_cnt >= @limit # there could be more to read
        sleep @query_interval if @query_interval # don't hammer database
      end 
      more_to_read    
    end
    
    # def _statement
      # unless @stmt
        # db = @db = SQLite3::Database.new(@db_file)
        # @db.type_translation = true   
        # table_name = t = @sname  
        # if @offset < 0
          # cnt = db.execute("select count(*) from #{table_name};")[0][0].to_i
          # #debug "CNT: #{cnt}.#{cnt.class} offset: #{@offset}"
          # @offset = cnt + @offset # @offset was negative here
          # debug("Initial offset #{@offset} in '#{table_name}' with #{cnt} rows")
          # @offset = 0 if @offset < 0
        # end
        # #@stmt = db.prepare("SELECT * FROM #{table_name} LIMIT ? OFFSET ?;")
        # @stmt = db.prepare("SELECT _senders.name, #{t}.* FROM #{t} JOIN _senders WHERE #{t}.oml_sender_id = _senders.id LIMIT ? OFFSET ?;")
      # end
      # @stmt
    # end
  end # OmlSqlRow


end

if $0 == __FILE__

  require 'omf_oml/table'
  ep = OMF::OML::OmlSqlSource.new('brooklynDemo.sq3')
  ep.on_new_stream() do |s|
    puts ">>>>>>>>>>>> New stream #{s.stream_name}: #{s.names.join(', ')}"
    case s.stream_name
    when 'wimaxmonitor_wimaxstatus'
      select = [:oml_ts_server, :sender_hostname, :frequency, :signal, :rssi, :cinr, :avg_tx_pw]
    when 'GPSlogger_gps_data'
      select = [:oml_ts_server, :oml_sender_id, :lat, :lon]
    end

    s.on_new_vector() do |v|
      puts "New vector(#{s.stream_name}): #{v.select(*select).join('|')}"      
    end
  end
  ep.run()

end

