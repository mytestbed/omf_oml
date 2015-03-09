#-------------------------------------------------------------------------------
# Copyright (c) 2015 National ICT Australia Limited (NICTA).
# This software may be used and distributed solely under the terms of the MIT license (License).
# You should find a copy of the License in LICENSE.TXT or at http://opensource.org/licenses/MIT.
# By downloading or using this software you accept the terms and the liability disclaimer in the License.
#-------------------------------------------------------------------------------
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
    COUNT_SCHEMA = OmlSchema.new([[:count, :int]])

    # Run 'query' on this database and return the resulting
    # rows to 'block'.
    #
    def run_query(query, limit, offset, schema)
      #puts ">>> RUN QUERY"
      cbk = AsyncHandler.new
      if (limit > 0 || offset > 0)
        query += " LIMIT #{limit} OFFSET #{offset}"
      end
      @pool.lease do |conn|
        #puts ">>>>> RUNING QUERY: #{query}"
        df = conn.query_defer(query)
        df.callback do |result|
          @pool.release(conn)
          #schema.cast_row
          rows = []
          result.each_row do |raw_row|
            rows << schema.cast_row(raw_row)
          end
          #puts ">>>> #{query[0 .. 60]} - #{rows.length}"
          Fiber.new do
            begin
              cbk.success rows
            rescue => ex
              warn "While 'onSucess' callback - #{ex}"
              debug ex.backtrace.join("\n")
            end
          end.resume
        end
        df.errback do |ex|
          @pool.release(conn)
          debug "SQL error: #{ex}"
          cbk.failure ex
         end
      end
      cbk
    end

    def run_count_query(query, onSuccess = nil, onFailure = nil, &block)
      cbk = AsyncHandler.new
      # TODO: This is is a bit fragile
      q = query.downcase.strip
      unless q.start_with? 'select'
        raise "Expected query to start with 'select' - #{query}"
      end
      parts = q.split("from")
      if parts.length < 2
        raise "Expected at least one FROM in query - #{query}"
      end
      parts.shift # discard first select part to be replaced by count(*)
      cq = "select count(*) from #{parts.join('from')}"
      debug "Count query: #{cq}"
      df = run_query(cq, 0, 0, COUNT_SCHEMA)
      df.onSuccess do |r|
        #puts "COUNT>>>>> #{r}"
        cbk.success(r[0][0])
      end
      df.onFailure do |e|
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
    end

    def _set_db_opts(db_opts)
      unless db_opts[:dbname] = db_opts[:database]
        raise "Missing database name - #{db_opts}"
      end

      opts = {}
      [:host, :port, :user, :password, :dbname].each do |k|
        v = db_opts[k]
        opts[k] = v if v
      end
      @pool = Pool.new(opts)
    end

  end # class

  class Pool < OMF::Base::LObject
    # Interval between we check low water mark and drop connections
    # down to that level
    #
    CLEANUP_INTERVAL = 10

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

    def lease(&block)
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
          _provision
        end
      end
    end

    # Add a resource to the pool
    def add(resource)
      release(resource)
    end

    def _provision()
      debug "Open new database connection - #{@db_opts}"
      df = PG::EM::Client.connect_defer(@db_opts)
      @provisioned += 1
      df.callback do |conn|
        info "Connected to database '#{@label}'"
        _release(conn, true)
      end
      df.errback do |ex|
        @provisioned -= 1
        error "Could NOT connect to database '#{ex}'"
      end
    end

    def initialize(db_opts, max_size = 4, max_waiting = 10)
      @db_opts = db_opts
      @label = "DB::#{db_opts[:host]}:#{db_opts[:port]}/#{db_opts[:dbname]}"
      @mutex = Mutex.new
      @content = []
      @waiting = []
      @max_size = max_size
      @max_waiting = max_waiting
      @provisioned = 0
      @outstanding = 0 # keep track of how many resources we really need
      @highwater = 0
      # seed
      #provision.call(self)

      EM.add_periodic_timer(CLEANUP_INTERVAL) do
        begin
          @mutex.synchronize do
            #puts "HIGHWATER #{@highwater}"
            if (@provisioned) > 0
              if (count = @provisioned - @highwater) > 0
                info "Closing #{count} out of #{@provisioned} database connections to '#{@label}'"
                count.times do
                  c = @content.shift
                  c.close
                  @provisioned -= 1
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
