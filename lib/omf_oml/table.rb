
require 'monitor'

require 'omf_common/lobject'
require 'omf_oml'
require 'omf_oml/schema'



module OMF::OML
          
  # This class represents a database like table holding a sequence of OML measurements (rows) according
  # a common schema.
  #
  class OmlTable < OMF::Common::LObject
    
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
    #   Table adds a '__id__' column at the beginning which keeps track of the rows unique id
    # opts -
    #   :max_size - keep table to that size by dropping older rows
    #   :index - only keep the latest inserted row for a unique col value - messes with row order
    #
    def initialize(tname, schema, opts = {}, &on_before_row_added)
      super tname
      
      #@endpoint = endpoint
      @name = tname
      @schema = OmlSchema.create(schema)
      unless @schema.name_at(0) == :__id__
        @schema.insert_column_at(0, [:__id__, 'int'])
      end
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
    def on_content_changed(key, offset = -1, &proc)
      #puts ">>>>>>> #{offset}"
      if proc
        @on_content_changed[key] = proc
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
    
    # NOTE: +on_row_added+ callbacks are done within the monitor. 
    #
    def add_row(row, needs_casting = false)
      synchronize do
        if row = _add_row(row, needs_casting)
          _notify_content_changed(:added, [row])
        end
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
    
    # Return a new table which only contains the rows of this
    # table whose value in column 'col_name' is equal to 'col_value'
    #
    def create_sliced_table(col_name, col_value, table_opts = {})
      sname = "#{@name}_slice_#{Kernel.rand}"

      st = self.class.new(name, @schema, table_opts)
      st.instance_variable_set(:@sname, sname)
      st.instance_variable_set(:@master_ds, self)
      def st.release
        @master_ds.on_content_changed(@sname) # release callback
      end

      index = @schema.index_for_col(col_name)
      on_content_changed(sname, 0) do |action, rows|
        if action == :removed
          warn "No support for removing rows from sliced table '#{sname}'."
          next
        end
        rows.each do |row|
          if row[index] == col_value
            row = row[1 .. -1] # remove the row_id
            debug "Add row '#{row.inspect}'"
            st.add_row(row)
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

      row.insert(0, @row_id += 1)
      _add_row_finally(row)
    end
    
    # Finally add 'row' to internal storage. This would be the method to
    # override in sub classes as this is thread safe and all other pre-storage
    # test have been performed. Should return the row added, or nil if nothing
    # was ultimately added.
    #
    def _add_row_finally(row)
      # if @indexed_rows
        # @indexed_rows[row[@index_col]] = row
        # return
      # end
      
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
        #puts "call: #{proc.inspect}"
        #if proc.arity == 1
          proc.call(action, rows)
        #else
          #proc.call(row, @offset)
        #end
      end
    end

  end # OMLTable

end
