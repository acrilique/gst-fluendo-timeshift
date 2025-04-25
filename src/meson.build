# List of source files for the plugin
timeshift_sources = [
  'gsttsindexer.c',
  'gsttsseeker.c',
  'gsttsshifter.c',
  'gsttsshifterbin.c',
  'gsttstimeshift.c',
  'tscache.c',
  'tsindex.c',
  'tsmemindex.c'
]

# Define PACKAGE for GST_PLUGIN_DEFINE internal use
project_defines = ['-DPACKAGE="gst-fluendo-timeshift"']

# Define the shared library for the GStreamer plugin
gsttstimeshift = shared_library('gsttstimeshift',
  timeshift_sources,
  c_args : project_defines, # Add the define
  dependencies : [gst_dep, gstbase_dep, gstpbutils_dep, gstpbad_dep], # Link against GStreamer libs
  link_args: ['-lgstmpegts-1.0'],
  install : true,
  install_dir : get_option('libdir') / 'gstreamer-1.0' # Standard GStreamer plugin install path
)
