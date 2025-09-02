
struct FieldCollector {
std::unordered_map<std::string, std::unique_ptr<arrow::StringBuilder>> builders;
std::vector<std::shared_ptr<arrow::Field>> fields;
arrow::MemoryPool* pool;
size_t row_count = 0;

FieldCollector(arrow::MemoryPool* pool) : pool(pool) {}

void add_field(const std::string& field_name) {
if (builders.find(field_name) == builders.end()) {
builders[field_name] = std::make_unique<arrow::StringBuilder>(pool);
fields.push_back(arrow::field(field_name, arrow::utf8()));
}
}

void add_value(const std::string& field_name, const std::string& value) {
builders[field_name]->Append(value);
}

void finish_row() {
// Add nulls to any fields not present in this row
for (auto& [field_name, builder] : builders) {
if (builder->length() <= row_count) {
builder->AppendNull();
}
}
row_count++;
}

std::shared_ptr<arrow::Table> finish() {
std::vector<std::shared_ptr<arrow::Array>> arrays;
for (auto& [field_name, builder] : builders) {
std::shared_ptr<arrow::Array> array;
builder->Finish(&array);
arrays.push_back(array);
}
auto schema = std::make_shared<arrow::Schema>(fields);
std::cout << row_count << std::endl;
return arrow::Table::Make(schema, arrays, row_count);
}
};
