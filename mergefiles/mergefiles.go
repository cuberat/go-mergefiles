// Copyright (c) 2018 Don Owens <don@regexguy.com>.  All rights reserved.
//
// This software is released under the BSD license:
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions
// are met:
//
//  * Redistributions of source code must retain the above copyright
//    notice, this list of conditions and the following disclaimer.
//
//  * Redistributions in binary form must reproduce the above
//    copyright notice, this list of conditions and the following
//    disclaimer in the documentation and/or other materials provided
//    with the distribution.
//
//  * Neither the name of the author nor the names of its
//    contributors may be used to endorse or promote products derived
//    from this software without specific prior written permission.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
// "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
// LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS
// FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE
// COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT,
// INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
// (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
// SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
// HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT,
// STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
// ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED
// OF THE POSSIBILITY OF SUCH DAMAGE.


package mergefiles

import (
    // "errors"
)

type stream_data struct {
    reader RecordReader
    last_key string
    last_rec interface{}
    done bool
}

type MergeFiles struct {
    readers []RecordReader
    writer RecordWriter
    merger RecordMerger
    streams []*stream_data
}

type HasRecFunc func() (bool)
type ReadRecFunc func() (key string, rec interface{}, done bool, err error)
type MergeRecFunc func(key string, recs []interface{}) (rec interface{}, err error)
type WriteRecFunc func(key string, rec interface{}) (err error)

func NewMergeFilesSlice(writer RecordWriter, merger RecordMerger,
    readers []RecordReader) (*MergeFiles) {

    mf := new(MergeFiles)
    mf.writer = writer
    mf.merger = merger
    mf.readers = readers

    mf.streams = make([]*stream_data, 0, len(mf.readers))
    for _, r := range mf.readers {
        d := new(stream_data)
        d.reader = r
    }

    return mf
}

func (mf *MergeFiles) comp_entry(key1 string, data1 interface{}, key2 string, data2 interface{}) int {
    switch {
    case key1 == key2:
        return 0
    case key1 < key2:
        return -1
    default:
        return 1
    }

    return 0
}

func (mf *MergeFiles) Merge() error {
    var (
        err error
        first_key string
        first_rec interface{}
    )

    // init
    for _, stream := range mf.streams {
        if stream.reader.HasRec() {
            stream.last_key, stream.last_rec, err = stream.reader.ReadRec()
            if err != nil {
                return err
            }
            first_key = stream.last_key
            first_rec = stream.last_rec
        } else {
            stream.done = true
        }
    }

    // main loop
    for true {
        found := false
        for _, stream := range mf.streams {
            if stream.done {
                continue
            }
            found = true
            first_key = stream.last_key
            first_rec = stream.last_rec
            break
        }

        if !found {
            // all done
            break
        }

        // find key to output
        for _, stream := range mf.streams {
            if stream.done {
                continue
            }

            rs := mf.comp_entry(stream.last_key, stream.last_rec,
                first_key, first_rec)
            if rs < 0 {
                first_key = stream.last_key
                first_rec = stream.last_rec
            }
        }

        // gather data for key to output
        recs := make([]interface{}, len(mf.streams))
        for idx, stream := range mf.streams {
            if stream.done {
                recs[idx] = nil
                continue
            }

            rs := mf.comp_entry(first_key, first_rec,
                stream.last_key, stream.last_rec)
            if rs == 0 {
                recs[idx] = stream.last_rec
                if stream.reader.HasRec() {
                    first_key, first_rec, err = stream.reader.ReadRec()
                    if err != nil {
                        return err
                    }
                } else {
                    stream.done = true
                }
                continue
            }

            recs[idx] = nil
        }

        // merge data for output
        out_rec, err := mf.merger.MergeRec(first_key, recs)
        if err != nil {
            return err
        }

        // output data
        err = mf.writer.WriteRec(first_key, out_rec)

    } // main loop

    return nil
}

func NewMergeFiles(writer RecordWriter, merger RecordMerger,
    reader ...RecordReader) (*MergeFiles) {
    return NewMergeFilesSlice(writer, merger, reader)
}

// RecordReader is the interface that wraps the HasRec and ReadRec methods.
//
// HasRec() returns true if there is another record to be read, and
// false otherwise.
//
// ReadRec() reads the next record. The key returned is the key
// used to compare two records. The rec value is the record read. If
// the error err is non-nil, processing will stop and err will be
// returned.
type RecordReader interface {
    HasRec() (bool)
    ReadRec() (key string, rec interface{}, done bool, err error)
}

// RecordMerger is the interface that wraps the MergeRec method.
//
// MergeRec() merges the provided records (one record per file/stream
// being processed), returning a single record with the data merged
// appropriately for output by the RecordWriter.
type RecordMerger interface {
    MergeRec(key string, recs []interface{}) (rec interface{}, err error)
}

// RecordWriter is the interface that wraps the WriteRec method.
//
// WriteRec writes out the provided record (the results from the
// RecordMerger).
type RecordWriter interface {
    WriteRec(key string, rec interface{}) (err error)
}

type Reader struct {
    has_rec HasRecFunc
    reader ReadRecFunc
}

func NewRecordReader(has_rec HasRecFunc, read_rec ReadRecFunc) (*Reader) {
    r := new(Reader)
    r.has_rec = has_rec
    r.reader = read_rec

    return r
}

func (r *Reader) HasRec() bool {
    return r.has_rec()
}

func (r *Reader) ReadRec() (key string, rec interface{}, err error) {
    return r.reader()
}

type Merger struct {
    merge_rec MergeRecFunc
}

func NewRecordMerger(merge_rec MergeRecFunc) (*Merger) {
    m := new(Merger)
    m.merge_rec = merge_rec

    return m
}

func (m *Merger) MergeRec(key string, recs []interface{}) (rec interface{}, err error) {
    return m.merge_rec(key, recs)
}

type Writer struct {
    write_rec WriteRecFunc
}

func NewRecordWriter(write_rec WriteRecFunc) (*Writer) {
    w := new(Writer)
    w.write_rec = write_rec

    return w
}

func (w *Writer) WriteRec(key string, rec interface{}) (err error) {
    return w.write_rec(key, rec)
}

