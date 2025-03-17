cc_library(
  name = 'concepts',
  hdrs = glob([
    'include/concepts/**/*.hpp',
  ]),
  includes = [
    "include",
  ],
)

cc_library(
  name = 'meta',
  hdrs = glob([
    'include/meta/**/*.hpp',
  ]),
  includes = [
    "include",
  ],
)

cc_library(
  name = 'std',
  hdrs = glob([
    'include/std/**/*.hpp',
  ]),
  includes = [
    "include",
  ],
)

cc_library(
  name = 'range-v3',
  hdrs = glob([
    'include/range/v3/**/*.hpp',
  ]),
  visibility = ["//visibility:public"],
  deps = [
    ':concepts',
    ':meta',
    ':std',
  ],
)
