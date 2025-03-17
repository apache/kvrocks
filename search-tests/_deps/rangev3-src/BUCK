prebuilt_cxx_library(
  name = 'concepts',
  header_namespace = 'concepts',
  header_only = True,
  exported_headers = subdir_glob([
    ('include/concepts', '**/*.hpp'),
  ]),
  licenses = [
    'LICENSE.txt',
  ],
)

prebuilt_cxx_library(
  name = 'meta',
  header_namespace = 'meta',
  header_only = True,
  exported_headers = subdir_glob([
    ('include/meta', '**/*.hpp'),
  ]),
  licenses = [
    'LICENSE.txt',
  ],
)

prebuilt_cxx_library(
  name = 'range-v3',
  header_namespace = 'range/v3',
  header_only = True,
  exported_headers = subdir_glob([
    ('include/range/v3', '**/*.hpp'),
  ]),
  licenses = [
    'LICENSE.txt',
  ],
  visibility = [
    'PUBLIC'
  ],
  deps = [
    ':concepts',
    ':meta',
  ],
)
