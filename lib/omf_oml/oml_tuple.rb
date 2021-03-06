#-------------------------------------------------------------------------------
# Copyright (c) 2013 National ICT Australia Limited (NICTA).
# This software may be used and distributed solely under the terms of the MIT license (License).
# You should find a copy of the License in LICENSE.TXT or at http://opensource.org/licenses/MIT.
# By downloading or using this software you accept the terms and the liability disclaimer in the License.
#-------------------------------------------------------------------------------
require 'omf_oml'
require 'omf_oml/schema'
require 'omf_oml/tuple'

module OMF::OML

  # This class represents a single vector from an OML measurement stream.
  # It provides various methods to access the vectors elements.
  #
  # NOTE: Do not store the vector itself, but make a copy as the instance may be
  # reused over various rows by the sender.
  #
  class OmlTuple < Tuple

    # Return the elements of the vector as an array
    # def to_a(include_index_ts = false)
      # res = []
      # r = @raw
      # if include_index_ts
        # res << @vprocs[:oml_ts].call(r)
        # res << @vprocs[:oml_seq_no].call(r)
      # end
      # @schema.each do |col|
        # res << @vprocs[col[:name]].call(r)
      # end
      # res
    # end

    def ts
      @raw[0].to_f
    end

    def seq_no
      @raw[1].to_i
    end

    # Note: This method assumes that the first two elements in each OML tuple,
    # 'oml_ts' and 'oml_seq_no' are not defined in the associated schema.
    #
    # def process_schema(schema)
      # i = 0
      # @vprocs = {}
      # schema.each_column do |col| #
        # name = col[:name] || raise("Ill-formed schema '#{schema}'")
        # type = col[:type] || raise("Ill-formed schema '#{schema}'")
        # j = i + 2; # need to create a locally scoped variable for the following lambdas
        # @vprocs[name] = @vprocs[i] = case type
          # when :string
            # lambda do |r| r[j] end
          # when :float
            # lambda do |r| r[j].to_f end
          # when :integer
            # lambda do |r| r[j].to_i end
          # else raise "Unrecognized OML type '#{type}' (#{col.inspect})"
        # end
        # i += 1
      # end
      # @vprocs[:oml_ts] = lambda do |r| r[0].to_f end
      # @vprocs[:oml_seq_no] = lambda do |r| r[1].to_i end
    # end

  end # OmlTuple
end # OMF::OML
