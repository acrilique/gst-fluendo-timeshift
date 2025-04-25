project('gst-fluendo-timeshift', 'c',
  version : '1.0.3',
  license : 'LGPL-2.0',
  meson_version : '>= 0.50.0',
  default_options : [ 'warning_level=1',
                      'c_std=gnu99'])

gst_req = '>= 1.0.0' # Minimum GStreamer version from configure.ac

gst_dep = dependency('gstreamer-1.0', version : gst_req)
gstbase_dep = dependency('gstreamer-base-1.0', version : gst_req)
gstpbutils_dep = dependency('gstreamer-plugins-base-1.0', version : gst_req)
gstpbad_dep = dependency('gstreamer-plugins-bad-1.0', version : gst_req)

# Build the source directory
subdir('src')

# Basic pkg-config file generation (optional but good practice)
pkg = import('pkgconfig')
pkg.generate(
  name : meson.project_name(),
  description : 'GStreamer Fluendo Time Shift element',
  version : meson.project_version(),
  libraries : [], # Plugin is loaded dynamically, not linked against
  requires : ['gstreamer-1.0', 'gstreamer-base-1.0', 'gstreamer-plugins-base-1.0', 'gstreamer-plugins-bad-1.0']
)
