
require 'monitor'

require 'omf_oml'
require 'omf_oml/schema'


module OMF::OML
          
  # This table maintains the most recently added
  # row with a unique entry in the +index+ column. 
  #
  class OmlIndexedTable < OmlTable
    
    # Shadow an existing table and maintain an index on 'index_col'.
    #
    # source_table - Table to shadow
    # index_col - Name of column to index on
    #
    def self.shadow(source_table, index_col, &on_before_row_added)
      name = "#{source_table.name}+#{index_col}"
      ix_table = self.new(name, index_col, source_table.schema, &on_before_row_added)
      source_table.on_row_added(self) do |r|
        ix_table.add_row(r)
      end
      ix_table
    end
    
    attr_reader :index_col
    
    # 
    # index_col - Name of column to index
    # schema - Table schema
    #
    def initialize(name, index_col, schema, &on_before_row_added)
      super name, schema, {}, &on_before_row_added
      @index_col = index_col
      @index2row = {} # each row is associated with an instance of the index
      @index = schema.index_for_col(index_col)
    end      
      
    def _add_row_finally(row)
      key = row[@index]
      row_id = @index2row[key]
      unless row_id
        row_id = @rows.length
        @index2row[key] = row_id
      end
      current_row = @rows[row_id]
      return nil if current_row == row
      
      if current_row 
        _notify_content_changed(:removed, [current_row])
      end
      @rows[row_id] = row
      return row
    end    

  end # class

end
