#-------------------------------------------------------------------------------
# Copyright (c) 2015 National ICT Australia Limited (NICTA).
# This software may be used and distributed solely under the terms of the MIT license (License).
# You should find a copy of the License in LICENSE.TXT or at http://opensource.org/licenses/MIT.
# By downloading or using this software you accept the terms and the liability disclaimer in the License.
#-------------------------------------------------------------------------------
require 'em-pg-client'
require 'omf_base/lobject'
require 'omf_oml/sql_source'
require 'omf_oml/async_handler'
require 'uri'
require 'em-pg-client'
require 'omf_oml/schema'

module OMF::OML

  # This class fetches the content of an SQL database and serves it as multiple
  # OML streams.
  #
  # After creating the object, the @run@ method needs to be called to
  # start producing the streams.
  #
  class OmlPostgresqlSource < OmlSqlSource
    # Interval between we check low water mark and drop connections
    # down to that level
    #
    CLEANUP_INTERVAL = 10

    # Interval between consecutive attempts to find a database on the
    # server. This is a problem with OML where the database only shows
    # up when the first OML client starts sending
    #
    DATABASE_REDISCOVER_INTERVAL = 10

    # Interval between consecutive attempts to find a table in a database.
    # This is a problem with OML where tables only show
    # up when the first OML relevant client starts sending
    #
    TABLE_REDISCOVER_INTERVAL = 10

    COUNT_SCHEMA = OmlSchema.new([[:count, :int]])

    # Run 'query' on this database and return the resulting
    # rows to 'block'.
    #
    def run_query(query, limit, offset, schema)
      original_query = query
      query = query.strip
      query = query[0 .. -2] if query.end_with?(';')
      cbk = AsyncHandler.new
      if (limit > 0 || offset > 0)
        query += " LIMIT #{limit} OFFSET #{offset}"
      end
      puts ">>> RUN  - #{query}"
      @pool.lease(cbk) do |conn|
        df = conn.query_defer(query)
        df.callback do |result|
          @pool.release(conn)
          puts ">>>>> ReESULT: #{result.inspect}"
          #schema.cast_row
          Fiber.new do
            begin
              rows = []
              result.each_row do |raw_row|
                #rows << schema.cast_row(raw_row)
                # Using PG::BasicTypeMapForResults to cast properly
                rows << raw_row
              end
          #puts ">>>> #{query[0 .. 60]} - #{rows.length}"
              cbk.success rows
            rescue => ex
              warn "While 'onSucess' callback - #{ex}"
              debug ex.backtrace.join("\n")
              cbk.failure(ex)
            end
          end.resume
        end
        df.errback do |ex|
          @pool.release(conn)
          _handle_query_error(ex, cbk) do
            cbk2 = run_query(original_query, limit, offset, schema)
            cbk.chain(cbk2)
          end
          # if (ex.is_a? PG::UndefinedTable) && @wait_for_tables
          #   m = ex.to_s.match(/relation "(.*)"/)
          #   table_name = m ? m[1] : '???'
          #   debug "Undefined Table - '#{table_name}'"
          #   since = @wait_for_tables_since[table_name] ||= Time.now
          #   if (Time.now - since < @wait_for_tables)
          #     # retry later
          #     debug "Retrying to find table '#{table_name}' in #{TABLE_REDISCOVER_INTERVAL} seconds"
          #     EM.add_timer TABLE_REDISCOVER_INTERVAL do
          #       cbk2 = run_query(original_query, limit, offset, schema)
          #       cbk.chain(cbk2)
          #     end
          #     next
          #   end
          # end
          # debug "SQL error(#{ex.class}): #{ex.to_s.gsub("\n", " ")}"
          # cbk.failure ex
         end
      end
      cbk
    end

    def _handle_query_error(ex, cbk, &block)
      if (ex.is_a? PG::UndefinedTable) && @wait_for_tables
        m = ex.to_s.match(/relation "(.*)"/)
        table_name = m ? m[1] : '???'
        debug "Undefined Table - '#{table_name}'"
        since = @wait_for_tables_since[table_name] ||= Time.now
        if (Time.now - since < @wait_for_tables)
          # retry later
          debug "Retrying to find table '#{table_name}' in #{TABLE_REDISCOVER_INTERVAL} seconds"
          EM.add_timer TABLE_REDISCOVER_INTERVAL, &block
          return
        end
      end
      debug "SQL error(#{ex.class}): #{ex.to_s.gsub("\n", " ")}"
      cbk.failure ex
    end

    def get_schema_for_query(query)
      original_query = query
      query = query.strip
      query = query[0 .. -2] if query.end_with?(';')
      cbk = AsyncHandler.new
      query += " LIMIT 0"
      @pool.lease(cbk) do |conn|
        df = conn.query_defer(query)
        df.callback do |result|
          @pool.release(conn)
          failed = false
          sa = [result.fields,
           result.type_map.build_column_map(result).coders
          ].transpose.map do |fname, coder|
            if coder.nil?
              # Not sure when that happens, but did
              cbk.failure "Psql driver couldn't work out encoding of result. No idea why? - #{query} - #{result.fields} - #{result.type_map.build_column_map(result).coders}"
              failed = true
              break
            end
            type = case coder.to_h[:name]
                     when /int/
                       :integer
                     when /float/
                       :double
                     when /text/
                       :text
                     else
                       warn "Unknown coder '#{coder.to_h}'"
                       :text
                   end
            [fname, type]
          end
          unless failed
            schema = OmlSchema.create sa
            cbk.success schema
          end
        end
        df.errback do |ex|
          @pool.release(conn)
          _handle_query_error(ex, cbk) do
            cbk2 = get_schema_for_query(original_query)
            cbk.chain(cbk2)
          end
        end
      end
      cbk
    end

    def run_count_query(query, onSuccess = nil, onFailure = nil, &block)
      cbk = AsyncHandler.new
      # TODO: This is is a bit fragile
      qp = query.gsub("\n", " ").match(/(select|SELECT)(.*?) (from|FROM) (.*)/)
      unless qp.length == 5
        raise "Expected query to start with 'select' and contain 'from' - #{query}"
      end
      cq = "SELECT count(*) FROM #{qp[4]}"
      debug "Count query: #{cq}"
      df = run_query(cq, 0, 0, COUNT_SCHEMA)
      df.on_success do |r|
        #puts "COUNT>>>>> #{r}"
        cbk.success(r[0][0])
      end
      df.on_failure do |e|
        cbk.failure e
      end
      cbk
    end

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


    # Return a Sequel Dataset from 'table_name'. See Sequel documentation on
    # what one can do with that.
    #
    # db_table_name Name of table in database
    #
    # def dataset(db_table_name)
    #   @db.from(db_table_name)
    # end
    #
    # def run_once()
    #   debug "Finding tables #{@db.tables}"
    #   # first find tables
    #   @db.tables.each do |tn|
    #     table_name = tn.to_s
    #     report_new_table(table_name) unless table_name.start_with?('_')
    #   end
    #   @tables
    # end

    def _preprocess_query(q)
      q
    end

    def _def_query_for_table(table_name)
      t = table_name.to_sym
      q = %{
        SELECT _senders.name AS oml_sender, a.*
        FROM #{t} AS a
        INNER JOIN _senders ON (_senders.id = a.oml_sender_id)
        ORDER BY oml_tuple_id;
      }
    end

    def initialize(db_opts, row_opts)
      super(db_opts, row_opts)
      @active_connection = 0
      @wait_for_tables_since = {}
    end

    def _set_db_opts(db_opts)
      unless db_opts[:dbname] = db_opts[:database]
        raise "Missing database name - #{db_opts}"
      end
      @wait_for_tables = db_opts[:wait_for_tables]

      dopts = {}
      [:host, :port, :user, :password, :dbname].each do |k|
        v = db_opts[k]
        dopts[k] = v if v
      end
      popts = {}
      [:wait_for_database].each do |k|
        v = db_opts[k]
        popts[k] = v if v
      end
      @pool = Pool.new(dopts, popts)
    end

  end # class

  class Pool < OMF::Base::LObject

    def release(el)
      _release(el, false)
    end

    def _release(el, newlyProvisioned)
      block = nil
      @mutex.synchronize do
        @outstanding -= 1 unless newlyProvisioned
        #puts "RELEASE>>> #{@outstanding} - #{el.object_id}"
        block = @waiting.shift
        unless block
          # nobody waiting
          @content.push(el)
        end
      end
      if block
        # reuse immediately
        EM.next_tick do
          block.call(el)
        end
      end
    end

    def lease(cbk = null, &block)
      el = nil
      @mutex.synchronize do
        el = @content.shift
        @outstanding += 1
        @highwater = @outstanding if @highwater < @outstanding
        #puts "LEASE>>> hw: #{@highwater} out: #{@outstanding} - - #{el.object_id}"
        unless el
          if @waiting.size < @max_waiting
            @waiting.push block
            if (pct = 100 * (w = @waiting.size) / @max_waiting) >= 75
              warn "Lease queue has reached #{pct}% capacity (#{w}/#{@max_waiting})"
            else
              debug "Lease requests pending: #{@waiting.size} (#{pct}%)"
            end
          else
            raise "Too many waiting"
          end
        end
      end
      if el
        block.call(el)
      else
        if @provisioned < @max_size
          _provision cbk
        end
      end
    end

    # Add a resource to the pool
    def add(resource)
      release(resource)
    end

    def _provision(cbk)
      debug "Open new database connection - #{@db_opts}"
      df = PG::EM::Client.connect_defer(@db_opts)
      @provisioned += 1
      df.callback do |conn|
        info "Connected to database '#{@label}'"
        # Assign a default ruleset for type casts of input and output values.
        conn.type_map_for_results = PG::BasicTypeMapForResults.new(conn)
        _release(conn, true)
      end
      df.errback do |ex|
        @provisioned -= 1
        if ex.is_a? PG::ConnectionBad
          case ex.to_s
            when /.*password authentication failed/
              cbk.failure('Password auth failed')
            when /.*database.*does not exist/
              if @wait_for_database && (Time.now - @created_at) < @wait_for_database
                # Retry later
                debug "Retrying to connect to database in #{OmlPostgresqlSource::DATABASE_REDISCOVER_INTERVAL} seconds"
                EM.add_timer OmlPostgresqlSource::DATABASE_REDISCOVER_INTERVAL do
                  _provision cbk
                end
                next
              end
              cbk.failure("X - Database doesn't exist - #{@wait_for_database}")
            else
            cbk.failure "Bad connection - #{ex.to_s}"
          end
        end
        debug "Provision error: #{ex.to_s.gsub("\n", " ")}"
        #cbk.failure(ex)
      end
    end

    # @opts opts [int] max_size Max number of simultaneous database connections
    # @opts opts [int] max_waiting Max. number of queries allowed to wait
    # @opts opts [int] wait_for_database Number of seconds to wait for the database to appear on server
    def initialize(db_opts, opts = {})
      @db_opts = db_opts
      if @wait_for_database = opts[:wait_for_database]
        @created_at = Time.now
      end
      @label = "DB::#{db_opts[:host]}:#{db_opts[:port]}/#{db_opts[:dbname]}"
      @mutex = Mutex.new
      @content = []
      @waiting = []
      @max_size = opts[:max_size] || 4
      @max_waiting = opts[:max_waiting] || 10
      @provisioned = 0
      @outstanding = 0 # keep track of how many resources we really need
      @highwater = 0
      # seed
      #provision.call(self)

      EM.add_periodic_timer(OmlPostgresqlSource::CLEANUP_INTERVAL) do
        begin
          @mutex.synchronize do
            #puts "HIGHWATER #{@highwater}"
            if (@provisioned) > 0
              if (count = @provisioned - @highwater) > 0
                info "Closing #{count} out of #{@provisioned} database connections to '#{@label}'"
                count.times do
                  if c = @content.shift
                    c.close
                    @provisioned -= 1
                  end
                end
              end
              debug "Maintaining #{@provisioned} connection(s) to '#{@label}'"
            end
            @highwater = 0
          end
        rescue => e
          warn "Exception while cleaning up DB connections - #{e}"
          debug e.backtrace.join("\n")
        end
      end
    end
  end # class
end # module

