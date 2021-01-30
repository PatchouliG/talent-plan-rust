## overview
### compact
1. append to log file
2. when the log file reach compact threshold, make it read only and mark it as need compacted, write to new log file
3. compact all log file to one file in another thread, rename when compact finish, update index in memory,
delete unused log file

#### load db
if compact is finish, rm log file, or discard the compacted output file
start compact

### file state
all file has unique id

current_file_id: for write
read_only_file_id: read only, need to be compact
compact_output_file_(read_file_id): name as compacting_{}
compact_finished_file_(read_only_files_id)

### code design