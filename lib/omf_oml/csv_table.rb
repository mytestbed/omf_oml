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
    # def self.create(tname, file_name, opts = {}, &on_before_row_added)
    #   opts[:file_name] = file_name
    #   self.new(tname, opts[:schema], opts, &on_before_row_added)
    # end

    #
    # tname - Name of table
    # schema - OmlSchema or Array containing [name, type*] for every column in table
    #   Table adds a '__id__' column at the beginning which keeps track of the rows unique id
    # @params opts
    # @opts :file_name - Name of file to read data from
    # @opts :text - Text to parse
    # @opts :max_size - keep table to that size by dropping older rows
    # @opts :index - only keep the latest inserted row for a unique col value - messes with row order
    # @opts :has_csv_header If true use first row in file as schema descriptor
    # @opts :schema Schema associated with this table
    #
    def initialize(tname, schema, opts = {}, &on_before_row_added)
      csv_opts = {}
      csv_opts[:headers] = (opts[:has_csv_header] == true) #(opts.delete(:has_csv_header) == true)
      unless csv_opts[:headers]
        raise "Current implementation only works with CSV files which inlcude a schema description in the first line"
      end

      if file_name = opts[:file_name]
        unless File.readable?(file_name)
          raise "Can't read CSV file '#{file_name}'"
        end

        encoding =  opts[:encoding] #opts.delete(:encoding)
        mode =  "rb"
        mode << ":#{encoding}" if encoding
        idata = File.open(file_name, mode)
      elsif idata = opts[:text]
        # nothing else to do
      else
        raise "Don't know where to get content from."
      end
      csv = CSV.new(idata, csv_opts)

      unless schema = opts[:schema]
        unless csv_opts[:headers]
          raise "No schema given and ':has_csv_header' not set to capture schema from file header"
        end
        csv.shift # force reading the first row to have the header parsed
        #puts ">>>>>> FIRST>>> #{first_row.inspect}"
        #csv.shift.each do |h, v| puts "#{h} => #{v.class}" end
        schema = csv.headers.map do |c|
          c = c.encode('UTF-8', :invalid => :replace, :undef => :replace, :replace => '?')
          name, type = c.split(':')
          [name.strip, (type || 'string').strip]
        end
        puts ">>>>>>>>>>>>>>>>>>>>>> SCHEMA: #{schema}"
        csv.rewind # we had to grab that earlier to get schema
      end
      super tname, schema, opts

      # if first_row # from getting the CSV header
      #   first_row.insert(0, @row_id += 1) if @add_index
      #   @rows = [@schema.cast_row(first_row)]
      # end

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

    # Return a new table which only contains the rows of this
    # table whose value in column 'col_name' is equal to 'col_value'
    #
    def create_sliced_table(col_name, col_value, table_opts = {})
      sname = "#{@name}_slice_#{Kernel.rand(100000)}"
      st = OmlTable.new(sname, @schema, table_opts)
      index = @schema.index_for_col(col_name)
      first_row = true
      @rows.each do |row|
        if row[index] == col_value
          #row = row[1 .. -1] # remove the row_id
          debug "Add first row '#{row.inspect}'" if first_row
          st.add_row(row)
          first_row = false
        end
      end
      def st.release
        # do nothing
      end

      debug "Created sliced table '#{sname}' from '#{@name}' (rows: #{st.rows.length} from #{@rows.length})"
      st
    end


  end # class
end # module
