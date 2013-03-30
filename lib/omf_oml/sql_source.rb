
require 'sequel'

require 'omf_common/lobject'
require 'omf_oml/endpoint'
require 'omf_oml/tuple'
require 'omf_oml/sql_row'

module OMF::OML

  # This class fetches the content of an SQL database and serves it as multiple
  # OML streams.
  #
  # After creating the object, the @run@ method needs to be called to
  # start producing the streams.
  #
  class OmlSqlSource < OMF::Common::LObject
    
    # Sequel adaptors sometimes don't return a :type identifier,
    # but always return the :db_type. This is a list of maps which may not work
    # for all adaptors
    # 
    FALLBACK_MAPPING = {
      'UNSIGNED INTEGER' => :integer
    }

    # db_opts - Options used to create a Sequel adapter
    #
    # Sequel.connect(:adapter=>'postgres', :host=>'norbit.npc.nicta.com.au', :user=>'oml2', :password=>'omlisgoodforyou', :database=>'openflow-demo')
    #
    def initialize(db_opts, row_opts = {})
      @running = false
      @on_new_stream_procs = {}
      @tables = {}
      @db_opts = db_opts
      debug "Opening DB (#{db_opts})"
      @db = Sequel.connect(db_opts)
      debug "DB: #{@db.inspect}"
      @row_opts = row_opts
    end

    # Register a proc to be called when a new stream was
    # discovered on this endpoint.
    #
    def on_new_stream(key = :_, &proc)
      if proc
        @on_new_stream_procs[key] = proc
      else
        @on_new_stream_procs.delete key
      end
    end
    
    # Return a table (more precisely an OmlTable instance) fed from 
    # the content of a table 'table_name' in this database.
    #
    # table_name - Name of table in the SQL database
    # opts - 
    #   :include_oml_internals - Include OML 'header' columns [true]
    #   :name - name used for returned OML Table [table_name]
    #   All other options defined for OmlSqlRow#new
    #
    def create_table(table_name, opts = {})
      tn = opts.delete(:name) || table_name
      schema = _schema_for_table(table_name)
      r = OmlSqlRow.new(table_name, schema, @db, opts)
      r.to_table(tn, opts)
    end
    
    # Call 'block' for every row in 'table_name' table.
    #
    # table_name - Name of table in the SQL database
    # opts - 
    #   :include_oml_internals[boolean] - Include OML 'header' columns [true]
    #   :schema[Schema] Schema to use for creating row
    #   All other options defined for OmlSqlRow#new
    #
    def create_stream(table_name, opts = {}, &block)
      rschema = opts.delete(:schema)
      schema = _schema_for_table(table_name)
      r = OmlSqlRow.new(table_name, schema, @db, opts)
      ropts = {}
      ropts[:schema] = rschema if rschema
      r.to_stream(ropts, &block)
    end

    #
    # Run a query on the database and return the result as an OmlTable. The provided schema 
    # needs to describe the SQL queries result set. Unfortunately we can only do very little
    # sanity checks here
    #
    def query(sql, table_name, schema)
      tbl = OmlTable.create(table_name, schema)
      @db.fetch(sql).each do |row|
        tbl << schema.hash_to_row(row)
      end
      tbl
    end

    # Start checking the database for tables and create a new stream
    # by calling the internal +report_new_table+ method.
    # If +check_every+ > 0 continue checking every +check_every+ seconds
    # for new tables in the database, otherwise it's only checked once
    #
    def run(check_every = -1)
      if check_every <= 0
        run_once()
      else
        Thread.new do
          @running = true
          while (@running)
            begin
              run_once()
            rescue Exception => ex
              error "Exception in OmlSqlSource#run: #{ex}"
              debug "Exception in OmlSqlSource#run: #{ex.backtrace.join("\n\t")}"
            end
            sleep check_every
          end
        end
      end
    end

    protected

    def run_once()
      debug "Finding tables #{@db.tables}"
      # first find tables
      @db.tables.each do |tn|
        table_name = tn.to_s
        report_new_table(table_name) unless table_name.start_with?('_')
      end
      @tables
      
      # postgresql
      # SELECT tablename FROM pg_tables
      # WHERE tablename NOT LIKE Ôpg\\_%Õ
      # AND tablename NOT LIKE Ôsql\\_%Õ;       
    end



    # THis method is being called for every table detected in the database.
    # It creates a new +OmlSqlRow+ object with +opts+ as the only argument.
    # The tables is then streamed as a tuple stream.
    # After the stream has been created, each block registered with
    # +on_new_stream+ is then called with the new stream as its single
    # argument.
    #
    def report_new_table(table_name)
      unless table =  @tables[table_name] # check if already reported before
        debug "Found table: #{table_name}"
        schema = _schema_for_table(table_name)
        table = @tables[table_name] = OmlSqlRow.new(table_name, schema, @db, @row_opts)
        #table = @tables[table_name] = OmlSqlRow.new(table_name, @db.schema(table_name), @db_opts, self, @row_opts)
        @on_new_stream_procs.each_value do |proc|
          proc.call(table)
        end
      end
      table
    end
    
    def _schema_for_table(table_name)
      begin
        schema_descr = @db.schema(table_name).map do |col_name, cd|
          unless type = cd[:type] || FALLBACK_MAPPING[cd[:db_type]]
            warn "Can't find ruby type for database type '#{cd[:db_type]}'"
          end
          {name: col_name, type: type}
        end
        #puts "SCHEMA_DESCR>>>> #{schema_descr}"
        schema = OmlSchema.new(schema_descr)
      rescue Sequel::Error => ex
        raise "Problems reading schema of table '#{table_name}'. Does it exist? (#{@db.tables})"
      end
    end
  end
  


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

