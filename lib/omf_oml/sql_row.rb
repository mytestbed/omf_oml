
require 'omf_oml/tuple'

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
    # table_name - name table in respective SQL database
    # schema_raw - a hash returned by the sequel library's #schema method
    # db_opts - Enough information to open a Sequel database adapter
    # source - Reference to the SqlSource which created this instance
    # opts:  
    #   - offset: Ignore first +offset+ rows. If negative or zero serve +offset+ rows initially
    #   - limit: Number of rows to fetch each time [1000]
    #   - check_interval: Interval in seconds when to check for new data. If 0, only run once.
    #
    def initialize(table_name, schema_raw, db_opts, source, opts = {})
      @sname = table_name
      @db_opts = db_opts
      @source = source
      
      unless @offset = opts[:offset]
        @offset = 0
      end
      @limit = opts[:limit]
      @limit = 1000 unless @limit
      
      @check_interval = opts[:check_interval]
      @check_interval = 0 unless @check_interval
      

      @on_new_vector_proc = {}

      schema = parse_schema(schema_raw)
      super table_name, schema 
    end
    
    
    # Return a specific element of the vector identified either
    # by it's name, or its col index
    #
    def [](name_or_index)
      @vprocs[name_or_index].call(@raw)
    end
    
    # Return the elements of the vector as an array
    def to_a(include_oml_internals = false)
      a = @schema.hash_to_row(@row, false, false) # don't need type conversion as sequel is doing this for us
      include_oml_internals ? a : a[4 .. -1]
    end
    
    # Return an array including the values for the names elements
    # given as parameters.
    #
    def select(*col_names)
      r = @row
      col_names.collect do |n|
        p = @vprocs[n]
        #puts "#{n}::#{p}"
        p ? p.call(r) : nil
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
    def capture_in_table(*args, &block)
      if args.length == 1
        if args[0].kind_of?(Array)
          select = args[0]
        elsif args[0].kind_of?(Hash)
          opts = args[0]
        end
      elsif args.length == 2 && args[1].kind_of?(Hash)
        select = args[0]
        opts = args[1]
      else
        opts = {}
        select = args
      end
      
      if (tschema = opts.delete(:schema))
        # unless tschema[0].kind_of? Hash
          # tschema = tschema.collect do |cname| {:name => cname} end
        # end 
      else
        tschema = select.collect do |cname| {:name => cname} end
      end
      tname = opts.delete(:name) || stream_name
      t = OMF::OML::OmlTable.new(tname, tschema, opts)
      if block
        self.on_new_tuple() do |v|
          #puts "New vector(#{tname}): #{v.schema.inspect} ---- #{v.select(*select).size} <#{v.select(*select).join('|')}>"
          if select
            row = block.call(v.select(*select))
          else
            row = block.call(v)
          end             
          if row
            raise "Expected kind of Array, but got '#{row.inspect}'" unless row.kind_of?(Array)
            t.add_row(row)
          end  
        end
      else
        self.on_new_tuple() do |v|
          #puts "New vector(#{tname}): #{v.select(*select).join('|')}"
          t.add_row(v.select(*select))   
        end
      end
      t
    end
    
    def to_table(name = nil, opts = {})
      unless name
        name = @sname
      end
      t = OMF::OML::OmlTable.new(name, self.schema)
      include_oml_internals = opts[:include_oml_internals] || true
      self.on_new_tuple() do |v|
        r = v.to_a(include_oml_internals)
        t.add_row(r)   
      end
      t
    end


    protected
        
    def parse_schema(raw)
      #puts ">> PARSE SCHEMA"
      sd = raw.collect do |col| 
        name, opts = col
        #puts col.inspect
        {:name => name, :type => opts[:db_type]}
      end
      # Query we are using is adding the 'oml_sender_name' to the front of the table
      sd.insert(0, :name => 'oml_sender', :type => :string)
      OmlSchema.new(sd)
    end
    
    # override
    def process_schema(schema)
      i = 0
      @vprocs = {}
      schema.each_column do |col|
        name = col[:name]
        j = i + 0
        l = @vprocs[name] = lambda do |r| r[j] end
        @vprocs[i - 4] = l if i > 4
        i += 1
      end
    end
    
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
      @db = Sequel.connect(@db_opts)
      
      if @check_interval <= 0
        _run_once
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
        #puts "ROW>>> #{r.inspect}"
        @row = r
        @on_new_vector_proc.each_value do |proc|
          proc.call(self)
        end
        row_cnt += 1
      end
      @offset += row_cnt
      debug "Read #{row_cnt}/#{@offset} rows from '#{@sname}'" if row_cnt > 0
      row_cnt >= @limit # there could be more to read     
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

