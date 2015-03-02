#-------------------------------------------------------------------------------
# Copyright (c) 2013 National ICT Australia Limited (NICTA).
# This software may be used and distributed solely under the terms of the MIT license (License).
# You should find a copy of the License in LICENSE.TXT or at http://opensource.org/licenses/MIT.
# By downloading or using this software you accept the terms and the liability disclaimer in the License.
#-------------------------------------------------------------------------------
require 'monitor'

require 'omf_base/lobject'
require 'omf_oml'
require 'omf_oml/schema'



module OMF::OML

  # This class represents a database like table holding a sequence of OML measurements (rows) according
  # a common schema.
  #
  class OmlTable < OMF::Base::LObject

    # @param opts
    # @opts :index Creates an index table (all rows have distinct values at column 'index')
    #
    def self.create(tname, schema, opts = {}, &on_before_row_added)
      if (index = opts.delete(:index))
        require 'omf_oml/indexed_table'
        OmlIndexedTable.new(tname, index, schema, &on_before_row_added)
      else
        OmlTable.new(tname, schema, opts, &on_before_row_added)
      end
    end
    include MonitorMixin

    attr_reader :name
    attr_accessor :max_size
    attr_reader :schema
    attr_reader :offset

    #
    # tname - Name of table
    # schema - OmlSchema or Array containing [name, type*] for every column in table
    #   Table adds a '__id__' column at the beginning which keeps track of the rows unique id unless
    #   option 'supress_index' is set.
    # opts -
    #   :max_size - keep table to that size by dropping older rows
    #   :supress_index - don't pex, even if schema doesn't start with '__id__'
    #
    def initialize(tname, schema, opts = {}, &on_before_row_added)
      super tname
      #@endpoint = endpoint
      @name = tname
      @schema = OmlSchema.create(schema)
      @add_index = false
      unless opts[:supress_index]
        unless @schema.name_at(0) == :__id__
          @add_index = true
          @schema.insert_column_at(0, [:__id__, 'int'])
        end
      end
      @col_count = @schema.columns.length
      @opts = opts
      if (index = opts[:index])
        throw "No longer supported, use IndexedTable instead"
        # @indexed_rows = {}
        # @index_col = @schema.index_for_col(index)
      end
      @on_before_row_added = on_before_row_added
      @offset = 0 # number of rows skipped before the first one recorded here
      @rows = []
      @row_id = 0 # Each new row is assigned an id
      @max_size = opts[:max_size]
      @on_content_changed = {}
    end

    def rows
      #@indexed_rows ? @indexed_rows.values : @rows
      @rows
    end

    # Register +callback+ to be called to process any newly
    # offered row before it being added to internal storage.
    # The callback's argument is the new row (TODO: in what form)
    # and should return what is being added instead of the original
    # row. If the +callback+ returns nil, nothing is being added.
    #
    def on_before_row_added(&callback)
      @on_before_row_added = callback
    end

    # Register callback for when the content of the table is changes. The key
    # allows for the callback to be removed by calling this method
    # without a block. . If the
    # optional 'offset' value is set to zero or a positive value,
    # then the currently stored values starting at this index are being
    # immediately sent to 'proc'. The 'proc' is expected to receive two
    # parameters, an 'action' and the content changed. The 'action' is either
    # ':added', or ':removed' and the content is an array of rows.
    #
    def on_content_changed(key = nil, offset = -1, &proc)
      #puts ">>>>>>> #{offset}"
      if proc
        @on_content_changed[key || proc] = proc
        if offset >= 0
          #with_offset = proc.arity == 2
          proc.call(:added, @rows[offset .. -1])
          #.each_with_index do |r, i|
            # with_offset ? proc.call(r, offset + i) : proc.call(r)
          # end
        end
      else
        @on_content_changed.delete key
      end
    end

    def on_row_added(key, &block)
      on_content_changed(key) do |action, rows|
        if action == :added
          rows.each {|r| block.call(r)}
        end
      end
    end

    # NOTE: +on_row_added+ callbacks are done within the monitor.
    #
    def add_row(row, needs_casting = false)
      synchronize do
        if row = _add_row(row, needs_casting)
          _notify_content_changed(:added, [row])
        end
      end
    end

    # Remove all rows from table.
    #
    # NOTE: +on_row_removed+ callbacks are done within the monitor.
    #
    def clear()
      synchronize do
        return if @rows.empty?
        old = @rows
        @rows = []
        #@row_id = 0 # Should we rest that as well??
        _notify_content_changed(:removed, old)
      end
    end


    def <<(row)
      add_row(row)
    end

    # Return a new table which shadows this table but only contains
    # rows with unique values in the column 'col_name' and of these the
    # latest added rows to this table.
    #
    # col_name - Name of column to use for indexing
    #
    def indexed_by(col_name)
      require 'omf_oml/indexed_table'
      OmlIndexedTable.shadow(self, col_name)
    end

    # Add an array of rows to this table
    #
    def add_rows(rows, needs_casting = false)
      synchronize do
        added = rows.map { |row| _add_row(row, needs_casting) }
        added = added.compact
        unless added.empty?
          _notify_content_changed(:added, added)
        end
      end
    end

    # Set the table to an array of rows, remove old
    # rows
    #
    def set_rows(rows, needs_casting = false)
      synchronize do
        oldSet = Set.new(@rows)
        @rows = []
        @row_id = 0
        @offset = 0
        added = rows.map { |row| _add_row(row, needs_casting) }
        added = added.compact
        newSet = Set.new(@rows)
        added = (newSet - oldSet).to_a
        unless added.empty?
          _notify_content_changed(:added, added)
        end
        removed = (oldSet - newSet).to_a
        unless removed.empty?
          _notify_content_changed(:removed, removed)
        end
        #puts ">>>> #{{added: added, removed: removed}}"
        #_notify_content_changed(:updated, {added: added, removed: removed})
      end
    end


    # Return a new table which only contains the rows of this
    # table whose value in column 'col_name' is equal to 'col_value'
    #
    def create_sliced_table(col_name, col_value, table_opts = {})
      sname = "#{@name}_slice_#{Kernel.rand}"

      st = self.class.new(name, @schema, @opts.merge(table_opts))
      st.instance_variable_set(:@sname, sname)
      st.instance_variable_set(:@master_ds, self)
      st.instance_variable_set(:@add_index, true)
      def st.release
        @master_ds.on_content_changed(@sname) # release callback
      end

      index = @schema.index_for_col(col_name)
      on_content_changed(sname, 0) do |action, rows|
        if action == :removed
          warn "No support for removing rows from sliced table '#{sname}'."
          next
        end
        first_row = true
        rows.each do |row|
          if row[index] == col_value
            row = row[1 .. -1] # remove the row_id
            debug "Add first row '#{row.inspect}'" if first_row
            st.add_row(row)
            first_row = false
          end
        end
      end
      debug "Created sliced table from '#{@name}' (rows: #{st.rows.length}-#{@rows.length})"
      st
    end

    # Return table as an array of rows
    #
    def to_a
      @rows.dup
    end

    def describe()
      rows
    end

    def data_sources
      self
    end

    private

    # NOT synchronized
    #
    def _add_row(row, needs_casting = false)
      throw "Expected array, but got '#{row}'" unless row.is_a?(Array)
      if needs_casting
        row = @schema.cast_row(row, true)
      end
      #puts row.inspect
      if @on_before_row_added
        row = @on_before_row_added.call(row)
      end
      return nil unless row

      row.insert(0, @row_id += 1) if @add_index
      _add_row_finally(row)
    end

    # Finally add 'row' to internal storage. This would be the method to
    # override in sub classes as this is thread safe and all other pre-storage
    # test have been performed. Should return the row added, or nil if nothing
    # was ultimately added.
    #
    def _add_row_finally(row)
      unless row.length == @col_count
        raise "Unexpected col count for row '#{row}' - schema: #{@schema}"
      end

      @rows << row
      if @max_size && @max_size > 0 && (s = @rows.size) > @max_size
        if (removed_row = @rows.shift) # not necessarily fool proof, but fast
          _notify_content_changed(:removed, [removed_row])
        end
        @offset = @offset + 1
      end
      row
    end

    def _notify_content_changed(action, rows)
      @on_content_changed.each_value do |proc|
        proc.call(action, rows)
      end
    end

  end # OMLTable

end
