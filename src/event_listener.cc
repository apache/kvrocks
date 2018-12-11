#include "event_listener.h"

void CompactionEventListener::OnCompactionCompleted(rocksdb::DB *db, const rocksdb::CompactionJobInfo &ci) {
  LOG(INFO) << "[Compaction compeleted] column family: " << ci.cf_name
            << ", base input level(files): " << ci.base_input_level << "(" << ci.input_files.size() << ")"
            << ", output level(files): " << ci.output_level << "(" << ci.output_files.size() << ")"
            << ", input bytes: " << ci.stats.total_input_bytes
            << ", output bytes:" << ci.stats.total_output_bytes
            << ", is_maunal:" << ci.stats.is_manual_compaction
            << ", elapsed(micro): " << ci.stats.elapsed_micros;
}