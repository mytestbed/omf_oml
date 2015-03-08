#-------------------------------------------------------------------------------
# Copyright (c) 2013 National ICT Australia Limited (NICTA).
# This software may be used and distributed solely under the terms of the MIT license (License).
# You should find a copy of the License in LICENSE.TXT or at http://opensource.org/licenses/MIT.
# By downloading or using this software you accept the terms and the liability disclaimer in the License.
#-------------------------------------------------------------------------------
require 'sequel'

require 'omf_base/lobject'
require 'omf_oml/endpoint'
require 'omf_oml/tuple'
require 'omf_oml/sql_row'
require 'uri'
require 'weakref'

module OMF::OML

  # This class fetches the content of an SQL database and serves it as multiple
  # OML streams.
  #
  # After creating the object, the @run@ method needs to be called to
  # start producing the streams.
  #
  class OmlSqlSource < OMF::Base::LObject
    # Interval between attempts to stop OmlRows using this source
    # from running if there is no one using hte associated tables
    # anymore
    CLEANUP_INTERVAL = 5

    @@sources = {}
    @@all_tables = []

    # db_opts - Options used to create a Sequel adapter
    #
    # Sequel.connect(:adapter=>'postgres', :host=>'norbit.npc.nicta.com.au', :user=>'oml2', :password=>'omlisgoodforyou', :database=>'openflow-demo')
    #
    def self.create(db_opts, row_opts = {})
      if source = @@sources[db_opts]
        return source
      end
      key = db_opts
      if db_opts.is_a? String
        url = URI(db_opts)
        db_opts = {adapter: url.scheme, host: url.host, url: db_opts}
        db_opts[:port] = url.port if url.port
        db_opts[:user] = url.user if url.user
        db_opts[:password] = url.password if url.password
        db = url.path
        if db.start_with? '/'
          db = db[1 .. -1]
        end
        if db.empty?
          raise "Missing database name - #{url}"
        end
        db_opts[:database] = db
      end
      case db_opts[:adapter].to_sym
        when :postgres
          require 'omf_oml/sql_postgresql_source'
          source = OmlPostgresqlSource.new(db_opts, row_opts)
        else
          raise "Can't figure out what database provider to use - #{db_opts}"
      end
      @@sources[key] = source
      source
    end

    # Periodically called to check if all the created tables are still
    # active.
    #
    def self.cleanup
      return unless @@all_tables.size > 0

      debug "Run cleanup"
      @@all_tables = @@all_tables.select do |t|
        next false unless t.weakref_alive?
        _cleanup_source(t)
        true
      end
    end

    def self._cleanup_source(st)
      return unless st.size > 0

      now = Time.now.to_i
      st.each do |ti|
        #puts ">> CHECKING SOME TABLE - #{ti.object_id}"
        # t: WeakRef.new(t), o: key, r: WeakRef.new(row), ts: Data.now.to_i
        table = ti[:t]
        next unless table

        if table.weakref_alive?
          #puts ">> CHECKING TABLE #{table} - #{table.observed?}"
          ti[:ts] = now if table.observed?

          if now - ti[:ts] > 10
            #puts ">>>>>>>>> RELEASE TABLE '#{table}' - #{ti.object_id}"
            row = ti[:r]
            if row && row.weakref_alive?
              debug "Stopping row '#{row}' for table '#{table}'"
              row.stop
            end
            ti.clear
            #puts ">>>>>>>>> RELEASED TABLE '#{table}' - #{ti} - #{ti.object_id}"
          end
        end
      end
    end

    EM.add_periodic_timer(CLEANUP_INTERVAL) do
      begin
        self.cleanup
      rescue => e
        warn "Exception while cleaning up sql sources - #{e}"
        debug e.backtrace.join("\n")
      end
    end

    # Sequel adaptors sometimes don't return a :type identifier,
    # but always return the :db_type. This is a list of maps which may not work
    # for all adaptors
    #
    FALLBACK_MAPPING = {
      'UNSIGNED INTEGER' => :integer,
      'UNSIGNED BIGINT' => :integer
    }

    # db_opts - Options used to create a Sequel adapter
    #
    # Sequel.connect(:adapter=>'postgres', :host=>'norbit.npc.nicta.com.au', :user=>'oml2', :password=>'omlisgoodforyou', :database=>'openflow-demo')
    #
    def initialize(db_opts, row_opts = {})
      @running = false
      @on_new_stream_procs = {}
      @tables = {}
      @oml_tables = []
      @@all_tables << WeakRef.new(@oml_tables)
      _set_db_opts(db_opts)
      #debug "DB: #{@db.inspect}"
      @row_opts = row_opts
    end
    protected :initialize

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
    #   :name - name used for returned OML Table [table_name]
    #   :schema - Schema to use instead of default table schema
    #   :query - Query to use instead of default one
    #   All other options defined for OmlSqlRow#new
    #
    def create_table(table_name, opts = {})
      opts[:name] ||= table_name
      #puts ">>> CREATE TABLE #{table_name} - #{opts}"
      t = @oml_tables.find do |el|
        el[:o] == opts && el[:t] && el[:t].weakref_alive?
      end
      if t
        table = t[:t].asSelf
        debug "Recycling table #{table} - #{t.object_id}"
        return table
      end

      key = opts.dup
      tn = opts.delete(:name) || table_name
      # TODO: A bit of a hack here to delay assigning a unique name
      tn = "Table#{rand(10**12)}" if (tn == '???')
      schema = opts.delete(:schema) || _schema_for_table(tn)
      q = opts.delete(:query) || _def_query_for_table(tn)
      query = _preprocess_query(q)
      #   query = (q.is_a? String) ? @db[q] : q
      # else
      #   query = _def_query_for_table(table_name)
      # end
      debug "Creating new table - #{opts}"
      r = OmlSqlRow.new(tn, schema, query, self, opts)
      opts[:schema] = schema
      t = r.to_table(tn, opts)
      def t.__sql_row__
        return self
      end
      @oml_tables << { t: WeakRef.new(t), o: key, r: WeakRef.new(r), ts: Time.now.to_i }
      t
    end

    # Call 'block' for every row in 'table_name' table.
    #
    # table_name - Name of table in the SQL database
    # opts -
    #   :schema[Schema] Schema to use for creating row
    #   All other options defined for OmlSqlRow#new
    # returns OmlSqlRow
    #
    def create_stream(table_name, opts = {}, &block)
      rschema = opts.delete(:schema)
      schema = _schema_for_table(table_name)
      r = OmlSqlRow.new(table_name, schema, _def_query_for_table(table_name), opts)
      if block
        ropts = {}
        ropts[:schema] = rschema if rschema
        r.to_stream(ropts, &block)
      end
      r
    end

    #
    # Run a query on the database and return the result as an OmlTable. The provided schema
    # needs to describe the SQL queries result set. Unfortunately we can only do very little
    # sanity checks here
    #
    # def query(sql, table_name, schema)
    #   tbl = OmlTable.create(table_name, schema)
    #   @db.fetch(sql).each do |row|
    #     tbl << schema.hash_to_row(row)
    #   end
    #   tbl
    # end

    #
    # Run a query on the database and return the result as an OmlTable. The provided schema
    # needs to describe the SQL queries result set. Unfortunately we can only do very little
    # sanity checks here. The query will be defined in the provided block which is passed in
    # the Sequel Database object and is expected to return a Sequel Dataset instance.
    #
    # def query2(table_name, schema, &block)
    #   tbl = OmlTable.create(table_name, schema)
    #   q = block.call(@db)
    #   unless q.is_a? Sequel::Dataset
    #     raise "Expected a Sequel::Dataset object, but got '#{q.class}'"
    #   end
    #   q.each do |row|
    #     tbl << tbl.schema.hash_to_row(row)
    #   end
    #   tbl
    # end

    # # Return a Sequel Dataset from 'table_name'. See Sequel documentation on
    # # what one can do with that.
    # #
    # # db_table_name Name of table in database
    # #
    # def dataset(db_table_name)
    #   @db.from(db_table_name)
    # end

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

    # def run_once()
    #   debug "Finding tables #{@db.tables}"
    #   # first find tables
    #   @db.tables.each do |tn|
    #     table_name = tn.to_s
    #     report_new_table(table_name) unless table_name.start_with?('_')
    #   end
    #   @tables
    # end



    # THis method is being called for every table detected in the database.
    # It creates a new +OmlSqlRow+ object with +opts+ as the only argument.
    # The tables is then streamed as a tuple stream.
    # After the stream has been created, each block registered with
    # +on_new_stream+ is then called with the new stream as its single
    # argument.
    #
    # def report_new_table(table_name)
    #   unless table =  @tables[table_name] # check if already reported before
    #     debug "Found table: #{table_name}"
    #     schema = _schema_for_table(table_name)
    #     query = _def_query_for_table(table_name)
    #     table = @tables[table_name] = OmlSqlRow.new(table_name, schema, query, @row_opts)
    #     #table = @tables[table_name] = OmlSqlRow.new(table_name, @db.schema(table_name), @db_opts, self, @row_opts)
    #     @on_new_stream_procs.each_value do |proc|
    #       proc.call(table)
    #     end
    #   end
    #   table
    # end
    #
    # def _schema_for_table(table_name)
    #   #raise ">>>SCHEMA"
    #   begin
    #     schema_descr = @db.schema(table_name).map do |col_name, cd|
    #       unless type = cd[:type] || FALLBACK_MAPPING[cd[:db_type]]
    #         warn "Can't find ruby type for database type '#{cd[:db_type]}'"
    #       end
    #       if col_name == :oml_sender_id
    #         # see _def_query_for_table(table_name) which replaces sender_id by sender name
    #         col_name = :oml_sender
    #         type = 'string'
    #       end
    #       {:name => col_name, :type => type}
    #     end
    #     #puts "SCHEMA_DESCR>>>> #{schema_descr}"
    #     schema = OmlSchema.new(schema_descr)
    #   rescue Sequel::Error => ex
    #     #raise "Problems reading schema of table '#{table_name}'. Does it exist? (#{@db.tables})"
    #     raise "Problems reading schema of table '#{table_name}'. Does it exist? - #{ex}"
    #   end
    # end

  #   def _def_query_for_table(table_name)
  #     t = table_name.to_sym
  #     @db["SELECT _senders.name as oml_sender, a.* FROM #{t} AS a INNER JOIN _senders ON (_senders.id = a.oml_sender_id) ORDER BY oml_tuple_id;"]
  #     @db[t].select(:_senders__name___oml_sender).select_all(t).select_append(:_senders__name___oml_sender) \
  #         .join('_senders'.to_sym, :id => :oml_sender_id).order(:oml_tuple_id)
  #     end
  end



end

if $0 == __FILE__
  OMF::Base::Loggable.init_log('sql_source_test')

  require 'omf_oml/table'
  db_file = File.join(File.dirname(__FILE__), '../../test/data/brooklynDemo.sq3')
  ep = OMF::OML::OmlSqlSource.new('sqlite://' + File.expand_path(db_file), :limit => 10)

  def on_new_stream(ep)
    ep.on_new_stream() do |s|
      puts ">>>>>>>>>>>> New stream #{s.stream_name}: #{s.schema.names}"
      case s.stream_name
      when 'wimaxmonitor_wimaxstatus'
        select = [:oml_ts_server, :sender_hostname, :frequency, :signal, :rssi, :cinr, :avg_tx_pwr]
      when 'GPSlogger_gps_data'
        select = [:oml_ts_server, :oml_sender_id, :lat, :lon]
      end

      s.on_new_tuple() do |v|
        begin
          puts "New vector(#{s.stream_name}): #{v.select(*select).join('|')}"
        rescue Exception => ex
          puts "ERROR: #{ex}"
          abort
        end
      end
    end
    ep.run()
  end

  t = ep.query2('gps', [[:lat, :float], [:lon, :float]]) do |db|
    db.from('GPSlogger_gps_data').select(:lat, :lon).limit(2)
  end
  puts t.rows.inspect
  puts t.schema

  # Raw query on database
  puts ep.dataset('GPSlogger_gps_data').select(:lat, :lon).limit(2).all.inspect

end

