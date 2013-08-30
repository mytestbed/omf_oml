#-------------------------------------------------------------------------------
# Copyright (c) 2013 National ICT Australia Limited (NICTA).
# This software may be used and distributed solely under the terms of the MIT license (License).
# You should find a copy of the License in LICENSE.TXT or at http://opensource.org/licenses/MIT.
# By downloading or using this software you accept the terms and the liability disclaimer in the License.
#-------------------------------------------------------------------------------
require 'csv'
require 'omf_oml'
require 'omf_oml/table'



module OMF::OML

  # This class represents a table whose content is initially stored in a
  # CSV file.
  #
  class OmlCsvTable < OmlTable

    # @param opts
    #
    def self.create(tname, file_name, opts = {}, &on_before_row_added)
      self.new(tname, file_name, opts, &on_before_row_added)
    end

    #
    # tname - Name of table
    # schema - OmlSchema or Array containing [name, type*] for every column in table
    #   Table adds a '__id__' column at the beginning which keeps track of the rows unique id
    # @params opts
    # @opts :max_size - keep table to that size by dropping older rows
    # @opts :index - only keep the latest inserted row for a unique col value - messes with row order
    # @opts :has_csv_header If true use first row in file as schema descriptor
    # @opts :schema Schema associated with this table
    #
    def initialize(tname, file_name, opts = {}, &on_before_row_added)
      unless File.readable?(file_name)
        raise "Can't read CSV file '#{file_name}'"
      end
      csv_opts = {}
      csv_opts[:headers] = (opts.delete(:has_csv_header) == true)
      unless csv_opts[:headers]
        raise "Current implementation only works with CSV files which inlcude a schema description in the first line"
      end

      encoding =  opts.delete(:encoding)
      mode =  "rb"
      mode << ":#{encoding}" if encoding
      csv = CSV.open(file_name, mode, csv_opts)

      unless schema = opts.delete(:schema)
        unless csv_opts[:headers]
          raise "No schema given and ':has_csv_header' not set to capture schema from file header"
        end
        first_row = csv.shift.fields # force reading the first row to have the header parsed
        #csv.shift.each do |h, v| puts "#{h} => #{v.class}" end
        schema = csv.headers.map do |c|
          c = c.encode('UTF-8', :invalid => :replace, :undef => :replace, :replace => '?')
          name, type = c.split(':')
          [name, type || 'string']
        end
      end
      super tname, schema, opts

      if first_row # from getting the CSV header
        first_row.insert(0, @row_id += 1) if @add_index
        @rows = [@schema.cast_row(first_row)]
      end

      # This assumes that CSV reader is setup with proper schema converters
      csv.each do |r|
        # Convert any strange strings into a clean ruby string
        row = r.fields.map do |e|
          e ? e.encode('UTF-8', :invalid => :replace, :undef => :replace, :replace => '?') : nil
        end
        row.insert(0, @row_id += 1) if @add_index
        @rows << @schema.cast_row(row)
      end

    end



  end # class
end # module
