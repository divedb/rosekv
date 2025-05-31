#pragma once

#include <system_error>

namespace rosekv {

enum class WALError {
  kTooLargeData,
};

class WALErrorCategory : public std::error_category {
 public:
  const char* name() const noexcept override { return "WALError"; }

  std::string message(int ev) const override {
    switch (static_cast<WALError>(ev)) {
      case WALError::kTooLargeData:
        return "Data size exceeds the segment's maximum allowed capacity.";

      default:
        return "Unknown WAL error";
    }
  }

  static const WALErrorCategory& instance() {
    static WALErrorCategory instance;

    return instance;
  }
};

std::error_code make_error_code(WALError e) {
  return {static_cast<int>(e), WALErrorCategory::instance()};
}

std::error_condition make_error_condition(WALError e) {
  return {static_cast<int>(e), WALErrorCategory::instance()};
}

}  // namespace rosekv

namespace std {

template <>
struct is_error_code_enum<rosekv::WALError> : true_type {};

}  // namespace std