#-------------------------------------------------------------------------------
# Copyright (c) 2013 National ICT Australia Limited (NICTA).
# This software may be used and distributed solely under the terms of the MIT license (License).
# You should find a copy of the License in LICENSE.TXT or at http://opensource.org/licenses/MIT.
# By downloading or using this software you accept the terms and the liability disclaimer in the License.
#-------------------------------------------------------------------------------
require 'omf_oml/oml_tuple'
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
    # @param [Sequel::Dataset] query - Databasequery to execute
    # @param [Hash] opts:
    #   - offset: Ignore first +offset+ rows. If negative or zero serve +offset+ rows initially
    #   - limit: Number of rows to fetch each time [1000]
    #   - check_interval: Interval in seconds when to check for new data. If < 0, only run once.
    #   - query_interval: Interval between consecutive queries when processing large result set.
    #
    def initialize(sql_table_name, schema, query, opts = {})
      debug "query:  #{query.sql}"
      @sname = sql_table_name
      @schema = schema
      raise "Expected OmlSchema but got '#{schema.class}'" unless schema.is_a? OmlSchema
      @query = query

      unless @offset = opts[:offset]
        @offset = 0
      end
      @limit = opts[:limit]
      @limit = 1000 unless @limit

      @check_interval = opts[:check_interval]
      @check_interval = -1 unless @check_interval
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

    # Return a table which will capture the content of this tuple stream.
    #
    # @param [string] name - Name to use for returned table
    # @param [Hash] opts Options to be passed on to Table constructor
    # @opts [boolean] opts :include_oml_internals If true will also include OML header columns
    # @opts [OmlSchema] opts :schema use specific schema for table (Needs to be a subset of the tuples schema)
    #
    def to_stream(opts = {}, &block)
      unless schema = opts.delete(:schema)
        include_oml_internals = (opts[:include_oml_internals] != false)
        schema = self.schema.clone(!include_oml_internals)
        if include_oml_internals
          # replace sender_id by sender ... see _run_once
          schema.replace_column_at 0, :oml_sender
        end
      end
      self.on_new_tuple(rand()) do |t|
        #v = t.to_a(schema)
        v = t.row
        block.arity == 1 ? block.call(v) : block.call(v, schema)
      end
      schema
    end

    # Return a table which will capture the content of this tuple stream.
    #
    # @param [string] name - Name to use for returned table
    # @param [Hash] opts Options to be passed on to Table constructor
    # @opts [boolean] opts :include_oml_internals If true will also include OML header columns
    # @opts [OmlSchema] opts :schema use specific schema for table (Needs to be a subset of the tuples schema)
    #
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
      self.on_new_tuple(t) do |v|
        r = v.to_a(schema)
        #puts r.inspect
        t.add_row(r)
      end
      t
    end

    def run(in_thread = true)
      return if @running
      if in_thread
        if Object.const_defined?("EM")
          if @check_interval <= 0
            Fiber.new do
              _run_once_quite
            end.resume
          else
            _run_periodic_em
          end
        else
          Thread.new do
            begin
              _run
            rescue Exception => ex
              error "Exception in OmlSqlRow: #{ex}"
              debug "Exception in OmlSqlRow: #{ex.backtrace.join("\n\t")}"
            end
          end
        end
      else
        _run
      end
    end

    private

    def _run_periodic_em
      outstanding = 0
      skipped = 0
      timer = EM.add_periodic_timer(@check_interval) do
        Fiber.new do
          #puts "CHECK START >>> #{outstanding}"
          if outstanding > 0
            debug "Skip periodic query, previous one hasn't finished yet"
            skipped += 1
            if (skipped > 10)
              # seem to have gotten stuck, retry again
              timer.cancel
              _run_periodic_em
            end
          else
            outstanding += 1
            t = Time.now
            begin
              _run_once
            rescue Sequel::DatabaseError => pex
              debug pex
              timer.cancel
              return _run_periodic_em
            rescue Exception => ex
              warn ex
              debug "#{ex.class}\t", ex.backtrace.join("\n\t")
              return true
            end
            outstanding -= 1
            skipped = 0
            debug "Sql query took #{Time.now - t} sec"
          end
        end.resume
      end
    end

    def _run
      if @check_interval <= 0
        #while _run_once; end
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

    def _run_once_quite
      begin
        return _run_once
      rescue Sequel::DatabaseError => pex
        debug pex
      rescue Exception => ex
        warn ex
        debug "#{ex.class}\t", ex.backtrace.join("\n\t")
        return true
      end
      false
    end

    # Run a query on database an serve all rows found one at a time.
    # Return true if there might be more rows in the database
    def _run_once
      row_cnt = 0
      t = table_name = @sname
      if (@offset < 0)
        cnt = @query.count()
        @offset = cnt + @offset # @offset was negative here
        debug("Initial offset #{@offset} in '#{table_name}' with #{cnt} rows")
        @offset = 0 if @offset < 0
      end
      #puts "QUERY>> #{@query.inspect}"
      @query.limit(@limit, @offset).each do |r|
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

  end # OmlSqlRow


end

if $0 == __FILE__

  OMF::Base::Loggable.init_log('sql_row_test')

  require 'omf_oml/sql_source'
  db_file = File.join(File.dirname(__FILE__), '../../test/data/brooklynDemo.sq3')
  ss = OMF::OML::OmlSqlSource.new('sqlite://' + File.expand_path(db_file))

  r = ss.create_stream('wimaxmonitor_wimaxstatus')
  r.run(false)
  puts '=============='
  puts r.row.inspect

end

