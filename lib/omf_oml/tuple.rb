#-------------------------------------------------------------------------------
# Copyright (c) 2013 National ICT Australia Limited (NICTA).
# This software may be used and distributed solely under the terms of the MIT license (License).
# You should find a copy of the License in LICENSE.TXT or at http://opensource.org/licenses/MIT.
# By downloading or using this software you accept the terms and the liability disclaimer in the License.
#-------------------------------------------------------------------------------
require 'omf_base/lobject'
require 'omf_oml'
require 'omf_oml/schema'
require 'omf_oml/table'

module OMF::OML

  # This class represents a tuple with an associated schema.
  # It provides various methods to access the tuple elements.
  #
  # NOTE: Do not store the tuple itself, but make a copy as the instance may be
  # reused over various rows by the sender.
  #
  # Use +OmlTuple+ if the schema is an OML one. +OmlTuple+ has additional convenience
  # methods.
  #
  class Tuple < OMF::Base::LObject

    # Return a specific element of the tuple identified either
    # by it's name, or its col index
    #
    def [](name_or_index)
      @schema.cast_col(name_or_index, @raw)
    end

    # Return the elements of the tuple as an array
    def to_a()
      @schema.cast_row(@raw)
    end

    # Return an array including the values for the names elements
    # given as parameters.
    #
    def select(*col_names)
      r = @raw
      col_names.map do |n|
        self[n]
      end
    end

    # Return a table (more precisely an OmlTable instance) fed from
    # the content of this tuple stream.
    #
    # table_name - Name of table
    # opts -
    #   All options defined for OmlTable#create
    #
    def create_table(table_name, opts = {})
      table = OmlTable.create(table_name, @schema, opts)
      id = -1
      on_new_tuple() do |t|
        row = @schema.cast_row(@raw, true)
        #puts "ROW>> #{row.inspect}"
        table.add_row(row)
      end
      table
    end

    attr_reader :schema
    attr_reader :stream_name

    def initialize(name, schema)
      # if schema.kind_of? Array
        # schema = OmlSchema.new(schema)
      # end
      @stream_name = name
      @schema = OmlSchema.create(schema)

      @raw = []
#      puts "SCHEMA: #{schema.inspect}"
      @on_new_tuple_procs = {}

      super name
      #process_schema(@schema)
    end


    # Parse the array of strings into the proper typed vector elements
    #
    # NOTE: We assume that each element is only called at most once, with some
    # never called. We therefore delay type-casting to the get function without
    # keeping the casted values (would increase lookup time)
    #
    def parse_tuple(els)
      @raw = els
      @on_new_tuple_procs.each_value do |proc|
        proc.call(self)
      end
    end

    # Register a proc to be called when a new tuple arrived
    #
    def on_new_tuple(key = :_, &proc)
      if proc
        @on_new_tuple_procs[key] = proc
      else
        @on_new_tuple_procs.delete key
      end
    end


    protected
    # def process_schema(schema)
      # i = 0
      # @vprocs = {}
      # schema.each_column do |col| #
        # name = col[:name] || raise("Ill-formed schema '#{schema}'")
        # type = col[:type] || raise("Ill-formed schema '#{schema}'")
        # @vprocs[name] = @vprocs[i] = case type
          # when :string
            # lambda do |r| r[i] end
          # when :float
            # lambda do |r| r[i].to_f end
          # else raise "Unrecognized Schema type '#{type}'"
        # end
        # i += 1
      # end
    # end

  end # Tuple
end # OMF::OML
