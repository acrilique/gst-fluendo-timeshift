#!/usr/bin/python

#
# GStreamer MPEG TS Time Shifting
# Copyright (C) 2011 Fluendo S.A. <support@fluendo.com>
#               2013 YouView TV Ltd. <krzysztof.konopko@youview.com>
#
# video-player.py: A simple GUI for playing TS streams.
#
# This library is free software; you can redistribute it and/or
# modify it under the terms of the GNU Library General Public
# License as published by the Free Software Foundation; either
# version 2 of the License, or (at your option) any later version.
#
# This library is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
# Library General Public License for more details.
#
# You should have received a copy of the GNU Library General Public
# License along with this library; if not, write to the
# Free Software Foundation, Inc., 51 Franklin St, Fifth Floor,
# Boston, MA 02110-1301, USA.
#

'''
Prticularily this scrip was devised to evaluate/demonstrate timeshifting.

It uses GObject introspection so the assumption is that GStreamer-1.0
introspection files are generated and made available to the Python process.
For example if Gstreamer-1.0 is installed in $HOME/opt/gstreamer-1.0,
the following variables should be available:

  LD_LIBRARY_PATH=$HOME/opt/gstreamer-1.0/lib:$LD_LIBRARY_PATH
  GI_TYPELIB_PATH=$HOME/opt/gstreamer-1.0/lib/girepository-1.0

'''

from os import path
import sys

import gi
gi.require_version('Gst', '1.0')
gi.require_version('Gtk', '3.0')
gi.require_version('GstVideo', '1.0')
from gi.repository import GObject, Gtk, Gst

# Needed for window.get_xid(), xvimagesink.set_window_handle(), respectively:
from gi.repository import GdkX11, GstVideo

Gst.init(None)

class Player(object):
    def __init__(self, src_description):
        self.position = Gst.CLOCK_TIME_NONE
        self.duration = Gst.CLOCK_TIME_NONE
        self.pcr_configured = False
        self.is_recording = False
        self.changed_id = -1

        self.window = Gtk.Window()
        self.window.connect('destroy', self.quit)
        self.window.set_default_size(800, 450)

        box = Gtk.Box()
        box.set_spacing (5)
        box.set_orientation(Gtk.Orientation.VERTICAL)
        self.window.add(box)

        self.drawingarea = Gtk.DrawingArea()
        box.pack_start(self.drawingarea, True, True, 0)

        hbox = Gtk.Box()
        hbox.set_spacing (5)
        hbox.set_orientation(Gtk.Orientation.HORIZONTAL)

        self.live_button = Gtk.Button(label='Live')
        def live_button_press_cb(widget, event):
            self.seek_end()
        self.live_button.connect('button-press-event', live_button_press_cb)

        hbox.pack_start(self.live_button, False, False, 0)

        self.pause_button = Gtk.Button(label='Pause')
        def pause_button_press_cb(widget, event):
            self.pause()
        self.pause_button.connect('button-press-event', pause_button_press_cb)

        hbox.pack_start(self.pause_button, False, False, 0)

        self.adjustment = Gtk.Adjustment(value=0.0, lower=0.0, upper=100.0, step_increment=0.1, page_increment=1.0, page_size=1.0)
        self.scale = Gtk.Scale(orientation=Gtk.Orientation.HORIZONTAL, adjustment=self.adjustment)
        self.scale.set_digits(0)
        self.scale.set_hexpand(True)
        self.scale.set_valign(Gtk.Align.START)
        self.scale.connect('button-press-event', self.scale_button_press_cb)
        self.scale.connect('button-release-event', self.scale_button_release_cb)
        self.scale.connect('format-value', self.scale_format_value_cb)

        hbox.pack_start(self.scale, False, True, 0)

        box.pack_start(hbox, False, False, 0)

        hbox2 = Gtk.Box()
        hbox2.set_spacing(5)
        hbox2.set_orientation(Gtk.Orientation.HORIZONTAL)

        self.label_duration = Gtk.Label(label='duration: --:--')
        hbox2.pack_start(self.label_duration, False, False, 0)

        self.label_buf_begin = Gtk.Label(label='buffer begin (bytes): 0')
        hbox2.pack_start(self.label_buf_begin, False, False, 0)

        self.label_buf_end = Gtk.Label(label='buffer end (bytes): 0')
        hbox2.pack_start(self.label_buf_end, False, False, 0)

        box.pack_start(hbox2, False, False, 0)

        # Create GStreamer pipeline
        self.pipeline = Gst.Pipeline()

        # Create bus to get events from GStreamer pipeline
        self.bus = self.pipeline.get_bus()
        self.bus.add_signal_watch()
        self.bus.connect('message::eos', self.on_eos)
        self.bus.connect('message::error', self.on_error)

        # This is needed to make the video output in our DrawingArea:
        self.bus.enable_sync_message_emission()
        self.bus.connect('sync-message::element', self.on_sync_message)

        ring_buffer_size = 512 * 1024 * 1024

        # Create GStreamer elements
        self.playbin = Gst.parse_bin_from_description(
            src_description + \
            ' ! queue ' \
            ' ! tsshifterbin name=timeshifterbin' \
                ' cache-size=%u' \
            ' ! decodebin ! autovideosink'
            % (ring_buffer_size),
            False);
        self.pipeline.add(self.playbin)

        ts = self.pipeline.get_by_name("timeshifter");
        if ts:
            def overrun_handler(obj):
                print ('Received overrun signal')
                buf_begin, _ = self.query_buffering()
                seek_to = buf_begin + 2 * Gst.SECOND
                print ('Will seek to %s' % self.format_time(seek_to))
                self.seek(seek_to)
                self.pipeline.set_state(Gst.State.PLAYING)

            ts.connect('overrun', overrun_handler)
            print ('Registered overrun handler')

        self.update_id = GObject.timeout_add(1000, self.update_scale_cb)

    def update_scale_cb(self):
        self.position, self.duration = self.query_position()
        self.label_duration.set_text('duration: %s' % self.format_time(self.duration))
        if Gst.CLOCK_TIME_NONE != self.position and 0 != self.duration:
            value = self.position * 100.0 / self.duration
            self.adjustment.set_value(value)

        buf_start, buf_end = self.query_buffering()
        self.label_buf_begin.set_text('buffer begin: %s' % self.format_time(buf_start))
        self.label_buf_end.set_text('buffer end: %s' % self.format_time(buf_end))

        return True

    def query_position(self):
        try:
            format, position = self.pipeline.query_position(Gst.Format.TIME)
        except:
            position = Gst.CLOCK_TIME_NONE

        try:
            format, duration = self.pipeline.query_duration(Gst.Format.TIME)
        except:
            duration = Gst.CLOCK_TIME_NONE

        return (position, duration)

    def query_buffering(self):
        buffering = Gst.Query.new_buffering(Gst.Format.TIME)
        if self.pipeline.query(buffering):
            _, buf_start, buf_end, _ = buffering.parse_buffering_range()
        else:
            print ('Buffering query failed')
            buf_start = 0
            buf_end = 0;

        return (buf_start, buf_end)

    def format_time(self, value):
        seconds = value / Gst.SECOND
        return '%02d:%02d' % (seconds / 60, seconds % 60)

    def scale_format_value_cb(self, scale, value):
        if Gst.CLOCK_TIME_NONE == self.duration:
            real = 0
        else:
            real = value * self.duration / 100

        return self.format_time(real)

    def run(self):
        self.window.show_all()
        # You need to get the XID after window.show_all().  You shouldn't get it
        # in the on_sync_message() handler because threading issues will cause
        # segfaults there.
        self.xid = self.drawingarea.get_property('window').get_xid()
        self.pipeline.set_state(Gst.State.PLAYING)

        Gtk.main()

    def quit(self, window):
        self.pipeline.set_state(Gst.State.NULL)
        Gtk.main_quit()

    def on_sync_message(self, bus, msg):
        s = msg.get_structure()

        if s.get_name() == 'prepare-window-handle':
            print('prepare-window-handle')
            msg.src.set_window_handle(self.xid)

    def seek(self, location):
        print('seeking to %r' % location)
        res = self.pipeline.seek(1.0, Gst.Format.TIME,
            Gst.SeekFlags.FLUSH | Gst.SeekFlags.ACCURATE,
            Gst.SeekType.SET, location,
            Gst.SeekType.NONE, 0)

        if res:
           print('setting new stream time to 0')
        else:
           print('seek to %r failed' % location)

    def seek_end(self):
        res = self.pipeline.seek(1.0, Gst.Format.TIME,
            Gst.SeekFlags.FLUSH | Gst.SeekFlags.ACCURATE,
            Gst.SeekType.END, -1,
            Gst.SeekType.NONE, 0)

        if res:
           print('setting new stream time to 0')
        else:
           print('seek to end failed')

    def pause(self):
        if self.pause_button.get_label() == 'Pause':
            self.pause_button.set_label('Play')
            state = Gst.State.PAUSED;
            print('pause button pressed - setting state to PAUSED')
        else:
            self.pause_button.set_label('Pause')
            state = Gst.State.PLAYING
            print('pause button pressed - setting state to PLAYING')

        self.pipeline.set_state(state)

    def scale_button_press_cb(self, widget, event):
        #print 'starting seek'

        # don't timeout-update position during seek
        if self.update_id != -1:
            GObject.source_remove(self.update_id)
            self.update_id = -1

        # make sure we get changed notifies
        if self.changed_id == -1:
            self.changed_id = self.scale.connect('value-changed',
                self.scale_value_changed_cb)

    def scale_value_changed_cb(self, scale):
        real = int(scale.get_value() * self.duration / 100) # in ns
        #print 'value changed, perform seek to %r' % real
        self.seek(real)
        # allow for a preroll
        self.pipeline.get_state(timeout = 50 * Gst.MSECOND)

    def scale_button_release_cb(self, widget, event):
        widget.disconnect(self.changed_id)
        self.changed_id = -1

        if self.update_id != -1:
            self.error('Had a previous update timeout id')
        else:
            self.update_id = GObject.timeout_add(1000, self.update_scale_cb)

    def on_eos(self, bus, msg):
        print('on_eos(): seeking to start of video')
        self.pipeline.seek_simple(
            Gst.Format.TIME,
            Gst.SeekFlags.FLUSH | Gst.SeekFlags.KEY_UNIT,
            0)

    def on_error(self, bus, msg):
        print('on_error():', msg.parse_error())

def main(argv):
    if len(sys.argv) == 1:
        '''
        Example stream (BBC One) can be generated as follows:

        gst-launch-0.10 -v \
          dvbsrc \
            bandwidth=8 code-rate-lp=NONE code-rate-hp=2/3 guard=32 \
            hierarchy=NONE modulation="QAM 64" trans-mode=8k \
            inversion=AUTO frequency=490000000 pids=100:101 symbol-rate=27500 \
          ! queue ! udpsink host=<target IP> port=10000

        '''
        src = 'udpsrc port=10000 caps="video/mpegts, media=(string)video, encoding-name=(string)MP2T-ES"'
    else:
        src = 'souphttpsrc location=%s' % (sys.argv[1])
    p = Player(src)
    p.run()

if __name__ == '__main__':
    sys.exit(main(sys.argv))
