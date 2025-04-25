/* GStreamer MPEG TS Time Shifting
 * Copyright (C) 2013 YouView TV Ltd. <krzysztof.konopko@youview.com>
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Library General Public
 * License as published by the Free Software Foundation; either
 * version 2 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Library General Public License for more details.
 *
 * You should have received a copy of the GNU Library General Public
 * License along with this library; if not, write to the
 * Free Software Foundation, Inc., 51 Franklin St, Fifth Floor,
 * Boston, MA 02110-1301, USA.
 */

#include "gsttsshifterbin.h"
#include "tscache.h"
#include "tsindex.h"
#include <gst/mpegts/mpegts.h>

GST_DEBUG_CATEGORY_EXTERN (ts_shifterbin);
#define GST_CAT_DEFAULT ts_shifterbin

G_DEFINE_TYPE (GstTSShifterBin, gst_ts_shifter_bin, GST_TYPE_BIN);

#define DEFAULT_MIN_CACHE_SIZE  (4 * CACHE_SLOT_SIZE)   /* 4 cache slots */
#define DEFAULT_CACHE_SIZE      (32 * 1024 * 1024)      /* 32 MB */

enum
{
  PROP_0,
  PROP_CACHE_SIZE,
  PROP_ALLOCATOR_NAME,
  PROP_LAST
};

static void gst_ts_shifter_bin_handle_message (GstBin * bin, GstMessage * msg);
static GstStateChangeReturn gst_ts_shifter_bin_change_state (GstElement * element, GstStateChange transition);

static GstStaticPadTemplate srctemplate = GST_STATIC_PAD_TEMPLATE ("src",
    GST_PAD_SRC,
    GST_PAD_ALWAYS,
    GST_STATIC_CAPS ("video/mpegts"));

static GstStaticPadTemplate sinktemplate = GST_STATIC_PAD_TEMPLATE ("sink",
    GST_PAD_SINK,
    GST_PAD_ALWAYS,
    GST_STATIC_CAPS ("video/mpegts"));

static void
gst_ts_shifter_bin_set_property (GObject * object,
    guint prop_id, const GValue * value, GParamSpec * pspec)
{
  GstTSShifterBin *ts_bin = GST_TS_SHIFTER_BIN (object);

  switch (prop_id) {
    case PROP_CACHE_SIZE:
      g_object_set_property (G_OBJECT (ts_bin->timeshifter),
          "cache-size", value);
      break;

    case PROP_ALLOCATOR_NAME:
      g_object_set_property (G_OBJECT (ts_bin->timeshifter),
          "allocator-name", value);
      break;

    default:
      G_OBJECT_WARN_INVALID_PROPERTY_ID (object, prop_id, pspec);
      break;
  }
}

static void
gst_ts_shifter_bin_get_property (GObject * object,
    guint prop_id, GValue * value, GParamSpec * pspec)
{
  GstTSShifterBin *ts_bin = GST_TS_SHIFTER_BIN (object);

  switch (prop_id) {
    case PROP_CACHE_SIZE:
      g_object_get_property (G_OBJECT (ts_bin->timeshifter),
          "cache-size", value);
      break;

    case PROP_ALLOCATOR_NAME:
      g_object_get_property (G_OBJECT (ts_bin->timeshifter),
          "allocator-name", value);
      break;

    default:
      G_OBJECT_WARN_INVALID_PROPERTY_ID (object, prop_id, pspec);
      break;
  }
}

static void
gst_ts_shifter_bin_class_init (GstTSShifterBinClass * klass)
{
  GObjectClass *gobject_class;
  GstElementClass *gstelement_class;
  GstBinClass *gstbin_class;

  gobject_class = G_OBJECT_CLASS (klass);
  gstelement_class = GST_ELEMENT_CLASS (klass);
  gstbin_class = GST_BIN_CLASS (klass);

  gobject_class->set_property = gst_ts_shifter_bin_set_property;
  gobject_class->get_property = gst_ts_shifter_bin_get_property;

  g_object_class_install_property (gobject_class, PROP_CACHE_SIZE,
      g_param_spec_uint64 ("cache-size",
          "Cache size in bytes",
          "Max. amount of data cached in memory (bytes)",
          DEFAULT_MIN_CACHE_SIZE, G_MAXUINT64, DEFAULT_CACHE_SIZE,
          G_PARAM_READWRITE | G_PARAM_STATIC_STRINGS));

  g_object_class_install_property (gobject_class, PROP_ALLOCATOR_NAME,
      g_param_spec_string ("allocator-name", "Allocator name",
          "The allocator to be used to allocate space for "
          "the ring buffer (NULL - default system allocator).",
          NULL, G_PARAM_READWRITE | G_PARAM_STATIC_STRINGS));

  gst_element_class_add_pad_template (gstelement_class,
      gst_static_pad_template_get (&srctemplate));

  gst_element_class_add_pad_template (gstelement_class,
      gst_static_pad_template_get (&sinktemplate));

  gstbin_class->handle_message =
      GST_DEBUG_FUNCPTR (gst_ts_shifter_bin_handle_message);

  gstelement_class->change_state =
      GST_DEBUG_FUNCPTR (gst_ts_shifter_bin_change_state);

  gst_element_class_set_static_metadata (gstelement_class,
      "Time Shift + TS parser for MPEG TS streams", "Generic/Bin",
      "Provide time shift operations on MPEG TS streams",
      "Krzysztof Konopko <krzysztof.konopko@youview.com>");
}

static void
mirror_pad (GstElement * element, const gchar * static_pad_name, GstBin * bin)
{
  GstPad *orig_pad, *ghost_pad;

  orig_pad = gst_element_get_static_pad (element, static_pad_name);
  g_return_if_fail (orig_pad);

  ghost_pad = gst_ghost_pad_new (static_pad_name, orig_pad);
  gst_object_unref (orig_pad);
  g_return_if_fail (ghost_pad);

  g_return_if_fail (gst_element_add_pad (GST_ELEMENT (bin), ghost_pad));
}

static void
gst_element_clear (GstElement ** elem)
{
  g_return_if_fail (!elem);
  if (*elem) {
    g_object_unref (G_OBJECT (*elem));
    *elem = NULL;
  }
}

static void
gst_ts_shifter_bin_perform_initial_seek (GstTSShifterBin * ts_bin)
{
  GstEvent *seek_event;
  
  if (ts_bin->initial_seek_performed)
    return;
    
  GST_INFO_OBJECT (ts_bin, "Performing initial seek to ensure proper timestamping");
  
  seek_event = gst_event_new_seek (1.0, GST_FORMAT_TIME, 
      GST_SEEK_FLAG_FLUSH, GST_SEEK_TYPE_END, -1, 
      GST_SEEK_TYPE_NONE, 0);
      
  /* Send the seek event to the seeker's src pad */
  if (gst_element_send_event (ts_bin->seeker, seek_event)) {
    GST_INFO_OBJECT (ts_bin, "Initial seek successful");
    ts_bin->initial_seek_performed = TRUE;
  } else {
    GST_WARNING_OBJECT (ts_bin, "Initial seek failed");
  }
}

static void
gst_ts_shifter_bin_init (GstTSShifterBin * ts_bin)
{
  GstIndex *index = NULL;
  GstBin *bin = GST_BIN (ts_bin);

  ts_bin->initial_seek_performed = FALSE;
  ts_bin->pipeline_is_playing = FALSE;

  ts_bin->parser = gst_element_factory_make ("tsparse", "parser");
  ts_bin->indexer = gst_element_factory_make ("tsindexer", "indexer");
  ts_bin->timeshifter = gst_element_factory_make ("tsshifter", "timeshifter");
  ts_bin->seeker = gst_element_factory_make ("tsseeker", "seeker");
  if (!ts_bin->parser || !ts_bin->indexer || !ts_bin->timeshifter
      || !ts_bin->seeker)
    goto error;

  gst_bin_add_many (bin, ts_bin->parser, ts_bin->indexer, ts_bin->timeshifter,
      ts_bin->seeker, NULL);
  g_return_if_fail (gst_element_link_many (ts_bin->parser, ts_bin->indexer,
          ts_bin->timeshifter, ts_bin->seeker, NULL));

  index = gst_index_factory_make ("memindex");
  g_object_set (G_OBJECT (ts_bin->indexer), "index", index, NULL);
  g_object_set (G_OBJECT (ts_bin->seeker), "index", index, NULL);
  g_object_unref (index);

  mirror_pad (ts_bin->parser, "sink", bin);
  mirror_pad (ts_bin->seeker, "src", bin);

  return;
error:
  gst_element_clear (&ts_bin->parser);
  gst_element_clear (&ts_bin->timeshifter);
  gst_element_clear (&ts_bin->seeker);
}

static GstStateChangeReturn
gst_ts_shifter_bin_change_state (GstElement * element, GstStateChange transition)
{
  GstTSShifterBin *ts_bin = GST_TS_SHIFTER_BIN (element);
  GstStateChangeReturn ret;

  ret = GST_ELEMENT_CLASS (gst_ts_shifter_bin_parent_class)->change_state (element, transition);
  if (ret == GST_STATE_CHANGE_FAILURE)
    return ret;

  switch (transition) {
    case GST_STATE_CHANGE_PAUSED_TO_PLAYING:
      ts_bin->pipeline_is_playing = TRUE;
      GST_INFO_OBJECT (ts_bin, "Pipeline reached PLAYING state");
      /* Check if we already have enough PCRs and need to seek now */
      if (!ts_bin->initial_seek_performed) {
        GST_INFO_OBJECT (ts_bin, "Enough PCRs received before PLAYING, performing initial seek now");
        gst_ts_shifter_bin_perform_initial_seek (ts_bin);
      }
      break;
    case GST_STATE_CHANGE_PLAYING_TO_PAUSED:
      ts_bin->pipeline_is_playing = FALSE;
      GST_INFO_OBJECT (ts_bin, "Pipeline left PLAYING state");
      break;
    default:
      break;
  }

  return ret;
}

static void
gst_ts_shifter_bin_handle_message (GstBin * bin, GstMessage * msg)
{
  GstTSShifterBin *ts_bin = GST_TS_SHIFTER_BIN (bin);

  if (gst_message_has_name (msg, "pmt")) {
    GstMpegtsSection *section;
    const GstMpegtsPMT *pmt;
    
    section = gst_message_parse_mpegts_section (msg);
    if (section) {
      pmt = gst_mpegts_section_get_pmt (section);
      if (pmt) {
        GST_DEBUG ("Setting PCR PID: %u", pmt->pcr_pid);
        g_object_set (ts_bin->indexer, "pcr-pid", pmt->pcr_pid, NULL);
      } else {
        GST_ERROR ("Failed to get PMT from section");
      }
      gst_mpegts_section_unref (section);
    } else {
      GST_ERROR ("Failed to parse MPEGTS section from message");
    }
  }

  GST_BIN_CLASS (gst_ts_shifter_bin_parent_class)
      ->handle_message (bin, msg);
}
