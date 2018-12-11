This is the implementation of locality-preserving range ORAMs as proposed in "rORAM: Efficient Range ORAM with O (log2N) Locality", to appear at Network and Distributed System Security (NDSS) 2019.

The code base is a fork of the original work by Bindschaedler et al. (http://seclab.soic.indiana.edu/curious/) and extends the framework to support locality-preserving range ORAMs. 

To run and evaluate our range ORAM implementation 

1. Run the compile script inside ./code/
2. To run experiments, use 
  
  ./run.sh <working_directory> <number_of_queries> RangeORAMRec LocalStorageSingleFile <max_range_size> <range_query_size_to_test>
  
  RangeORAMRec is the name of the ORAM client that interfaces with our range ORAM implementation
  LocalStorageSingleFile is a backend that stores the data distributed across multiple files. 
  
  For more details, please refer to the original code base. 
