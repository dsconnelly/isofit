#! /usr/bin/env python3
#
#  Copyright 2018 California Institute of Technology
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#
# ISOFIT: Imaging Spectrometer Optimal FITting
# Author: David R Thompson, david.r.thompson@jpl.nasa.gov
#

import os
import sys
import scipy as s
import pylab as plt
from scipy.linalg import inv, norm, sqrtm, det
from scipy.io import savemat
from inverse_simple import invert_simple
from spectral.io import envi
from common import load_spectrum, expand_all_paths
import logging
from collections import OrderedDict
from geometry import Geometry


# Constants related to file I/O
typemap = {s.uint8: 1, s.int16: 2, s.int32: 3, s.float32: 4, s.float64: 5,
           s.complex64: 6, s.complex128: 9, s.uint16: 12, s.uint32: 13, s.int64: 14,
           s.uint64: 15}
max_frames_size = 100
flush_rate = 100


class SpectrumFile:
    """A buffered file object that contains configuration information about
        formatting, etc."""

    def __init__(self, fname, write=False, n_rows=None, n_cols=None,
                 n_bands=None, interleave=None, dtype=s.float32,
                 wavelengths=None, fwhm=None, band_names=None,
                 bad_bands=[], zrange='{0.0, 1.0}',
                 ztitles='{Wavelength (nm), Magnitude}', map_info='{}'):

        self.frames = OrderedDict()
        self.write = write
        self.fname = fname
        self.wl = wavelengths
        self.band_names = band_names
        self.fwhm = fwhm
        self.n_rows = n_rows
        self.n_cols = n_cols
        self.n_bands = n_bands

        if self.fname.endswith('.txt'):

            # The .txt suffix implies a space-separated ASCII text file of
            # one or more data columns.  This is cheap to load and store, so
            # we do not defer read/write operations.
            logging.debug('Inferred ASCII file format for %s' % self.fname)
            self.format = 'ASCII'
            if not self.write:
                self.data,  self.wl = load_spectrum(self.fname)
                self.n_rows, self.n_cols, self.map_info = 1, 1, '{}'
                if self.wl is not None:
                    self.n_bands = len(self.wl)
                else:
                    self.n_bands = None
                self.meta = {}

        elif self.fname.endswith('.mat'):

            # The .mat suffix implies a matlab-style file, i.e. a dictionary
            # of 2D arrays and other matlab-like objects. This is typically
            # only used for specific output products associated with single
            # spectrum retrievals; there is no read option.
            logging.debug('Inferred MATLAB file format for %s' % self.fname)
            self.format = 'MATLAB'
            if not self.write:
                logging.error('Unsupported MATLAB file in input block')
                raise IOError('MATLAB format in input block not supported')

        else:

            # Otherwise we assume it is an ENVI-format file, which is
            # basically just a binary data cube with a detached human-
            # readable ASCII header describing dimensions, interleave, and
            # metadata.  We buffer this data in self.frames, reading and
            # writing individual rows of the cube on-demand.
            logging.debug('Inferred ENVI file format for %s' % self.fname)
            self.format = 'ENVI'

            if not self.write:

                # If we are an input file, the header must preexist.
                if not os.path.exists(self.fname+'.hdr'):
                    logging.error('Could not find %s' % (self.fname+'.hdr'))
                    raise IOError('Could not find %s' % (self.fname+'.hdr'))

                # open file and copy metadata, checking interleave format
                self.file = envi.open(self.fname + '.hdr', fname)
                self.meta = self.file.metadata.copy()
                if self.meta['interleave'] not in ['bil', 'bip']:
                    logging.error('Unsupported interleave format.')
                    raise IOError('Unsupported interleave format.')

                self.n_rows = int(self.meta['lines'])
                self.n_cols = int(self.meta['samples'])
                self.n_bands = int(self.meta['bands'])

            else:

                # If we are an output file, we may have to build the header
                # from scratch.  Hopefully the caller has supplied the
                # necessary metadata details.
                meta = {'lines': n_rows, 'samples': n_cols, 'bands': n_bands,
                        'byte order': 0, 'header offset': 0, 'map info': map_info,
                        'file_type': 'ENVI Standard', 'sensor type': 'unknown',
                        'interleave': interleave, 'data type': typemap[dtype],
                        'wavelength units': 'nm', 'z plot range': zrange,
                        'z plot titles': ztitles, 'fwhm': fwhm, 'bbl': bad_bands,
                        'band names': band_names, 'wavelength': self.wl}
                for k, v in meta.items():
                    if v is None:
                        logging.error('Must specify %s' % (k))
                        raise IOError('Must specify %s' % (k))

                self.file = envi.create_image(fname+'.hdr', meta, ext='',
                                              force=True)

            self.open_map_with_retries()

    def open_map_with_retries(self):
        """Try to open a memory map, handling Beowulf I/O issues"""
        self.memmap = None
        for attempt in range(10):
            self.memmap = self.file.open_memmap(interleave='source',
                                                writable=self.write)
            if self.memmap is not None:
                return
        raise IOError('could not open memmap for '+self.fname)

    def get_frame(self, row):
        """The frame is a 2D array, essentially a list of spectra.  The
            self.frames list acts as a hash table to avoid storing the
            entire cube in memory.  So we read them or create them on an
            as-needed basis.  When the buffer flushes via a call to
            flush_buffers, they will be deleted."""

        if row not in self.frames:
            if not self.write:
                d = self.memmap[row, :, :]
                if self.file.metadata['interleave'] == 'bil':
                    d = d.T
                self.frames[row] = d.copy()
            else:
                self.frames[row] = s.nan * s.zeros((self.n_cols, self.n_bands))
        return self.frames[row]

    def write_spectrum(self, row, col, x):
        """We write a spectrum.  If a binary format file, we simply change
           the data cached in self.frames and defer file I/O until
           flush_buffers is called."""

        if self.format == 'ASCII':

            # Multicolumn output for ASCII products
            s.savetxt(self.fname, x, fmt='%10.6f')

        elif self.format == 'MATLAB':

            # Dictionary output for MATLAB products
            savemat(self.fname, x)

        else:

            # Omit wavelength column for spectral products
            frame = self.get_frame(row)
            if x.ndim == 2:
                x = x[:, -1]
            frame[col, :] = x

    def read_spectrum(self, row, col):
        """Get a spectrum from the frame list or ASCII file.  Note that if
           we are an ASCII file, we have already read the single spectrum and
           return it as-is (ignoring the row/column indices)."""

        if self.format == 'ASCII':
            return self.data
        else:
            frame = self.get_frame(row)
            return frame[col]

    def flush_buffers(self):
        """Write to file, and refresh the memory map object"""
        if self.format == 'ENVI':
            if self.write:
                for row, frame in self.frames.items():
                    valid = s.logical_not(s.isnan(frame[:, 0]))
                    if self.file.metadata['interleave'] == 'bil':
                        self.memmap[row, :, valid] = frame[valid, :].T
                    else:
                        self.memmap[row, valid, :] = frame[valid, :]
            self.frames = OrderedDict()
            del self.file
            self.file = envi.open(self.fname+'.hdr', self.fname)
            self.open_map_with_retries()


class IO:

    def check_wavelengths(self, wl):
        """Make sure an input wavelengths align to the instrument
            definition"""

        return (len(wl) == self.fm.instrument.wl) and \
            all((wl-self.fm.instrument.wl) < 1e-2)

    def __init__(self, config, forward, inverse, active_rows, active_cols):
        """Initialization specifies retrieval subwindows for calculating
        measurement cost distributions"""

        # Default IO configuration options
        self.input = {}
        self.output = {'plot_surface_components': False}

        self.iv = inverse
        self.fm = forward
        self.bbl = '[]'
        self.radiance_correction = None
        self.meas_wl = forward.instrument.wl_init
        self.meas_fwhm = forward.instrument.fwhm_init
        self.writes = 0
        self.n_rows = 1
        self.n_cols = 1
        self.n_sv = len(self.fm.statevec)
        self.n_chan = len(self.fm.instrument.wl_init)

        if 'input' in config:
            self.input.update(config['input'])
        if 'output' in config:
            self.output.update(config['output'])
        if 'logging' in config:
            logging.config.dictConfig(config)

        # A list of all possible input data sources
        self.possible_inputs = ["measured_radiance_file",
                                "reference_reflectance_file",
                                "reflectance_file",
                                "obs_file",
                                "glt_file",
                                "loc_file",
                                "surface_prior_mean_file",
                                "surface_prior_variance_file",
                                "rt_prior_mean_file",
                                "rt_prior_variance_file",
                                "instrument_prior_mean_file",
                                "instrument_prior_variance_file",
                                "radiometry_correction_file"]

        # A list of all possible outputs.  There are several special cases
        # that we handle differently - the "plot_directory", "summary_file",
        # "Data dump file", etc.
        wl_names = [('Channel %i' % i) for i in range(self.n_chan)]
        sv_names = self.fm.statevec.copy()
        self.output_info = {
            "estimated_state_file":
                (sv_names,
                 '{State Parameter, Value}',
                 '{}'),
            "estimated_reflectance_file":
                (wl_names,
                 '{Wavelength (nm), Lambertian Reflectance}',
                 '{0.0,1.0}'),
            "estimated_emission_file":
                (wl_names,
                 '{Wavelength (nm), Emitted Radiance (uW nm-1 cm-2 sr-1)}',
                 '{}'),
            "modeled_radiance_file":
                (wl_names,
                 '{Wavelength (nm), Modeled Radiance (uW nm-1 cm-2 sr-1)}',
                 '{}'),
            "apparent_reflectance_file":
                (wl_names,
                 '{Wavelength (nm), Apparent Surface Reflectance}',
                 '{}'),
            "path_radiance_file":
                (wl_names,
                 '{Wavelength (nm), Path Radiance (uW nm-1 cm-2 sr-1)}',
                 '{}'),
            "simulated_measurement_file":
                (wl_names,
                 '{Wavelength (nm), Simulated Radiance (uW nm-1 cm-2 sr-1)}',
                 '{}'),
            "algebraic_inverse_file":
                (wl_names,
                 '{Wavelength (nm), Apparent Surface Reflectance}',
                 '{}'),
            "atmospheric_coefficients_file":
                (wl_names,
                 '{Wavelength (nm), Atmospheric Optical Parameters}',
                 '{}'),
            "radiometry_correction_file":
                (wl_names,
                 '{Wavelength (nm), Radiometric Correction Factors}',
                 '{}'),
            "spectral_calibration_file":
                (wl_names,
                 '{}',
                 '{}'),
            "posterior_uncertainty_file":
                (sv_names,
                 '{State Parameter, Value}',
                 '{}')}

        self.defined_outputs, self.defined_inputs = {}, {}
        self.infiles, self.outfiles, self.map_info = {}, {}, '{}'

        # Load input files and record relevant metadata
        for q in self.input:
            if q in self.possible_inputs:
                self.infiles[q] = SpectrumFile(self.input[q])

                if (self.infiles[q].n_rows > self.n_rows) or \
                   (self.infiles[q].n_cols > self.n_cols):
                    self.n_rows = self.infiles[q].n_rows
                    self.n_cols = self.infiles[q].n_cols

                for inherit in ['map info', 'bbl']:
                    if inherit in self.infiles[q].meta:
                        setattr(self, inherit.replace(' ', '_'),
                                self.infiles[q].meta[inherit])

        for q in self.output:
            if q in self.output_info:
                band_names, ztitle, zrange = self.output_info[q]
                n_bands = len(band_names)
                self.outfiles[q] = SpectrumFile(self.output[q], write=True,
                                                n_rows=self.n_rows, n_cols=self.n_cols,
                                                n_bands=n_bands, interleave='bip', dtype=s.float32,
                                                wavelengths=self.meas_wl, fwhm=self.meas_fwhm,
                                                band_names=band_names, bad_bands=self.bbl,
                                                map_info=self.map_info, zrange=zrange,
                                                ztitles=ztitle)

        # Do we apply a radiance correction?
        if 'radiometry_correction_file' in self.input:
            filename = self.input['radiometry_correction_file']
            self.radiance_correction, wl = load_spectrum(filename)

        # Last thing is to define the active image area
        if active_rows is None:
            active_rows = s.arange(self.n_rows)
        if active_cols is None:
            active_cols = s.arange(self.n_cols)
        self.iter_inds = []
        for row in active_rows:
            for col in active_cols:
                self.iter_inds.append([row, col])
        self.iter_inds = s.array(self.iter_inds)

        # Dave Connelly adds these lines to allow writing of full posterior
        # covariance matrices.
        if 'covariance_matrix_file' in self.output:
            self.covmat = self.output['covariance_matrix_file']
            self.mm = s.memmap(self.covmat, dtype='float32', mode='w+',
                shape=(self.n_rows, self.n_cols, self.n_sv, self.n_sv))

        # Dave Connelly adds this line to allow iteration outside for loops.
        self.iter = 0

    def flush_buffers(self):
        """ Write all buffered output data to disk, and erase read buffers."""

        for file_dictionary in [self.infiles, self.outfiles]:
            for name, fi in file_dictionary.items():
                fi.flush_buffers()

    def __iter__(self):
        """ Reset the iterator"""

        self.iter = 0
        return self

    def __next__(self):
        """ Get the next spectrum from the file.  Turn the iteration number
            into row/column indices and read from all input products."""

        if self.iter == len(self.iter_inds):
            self.flush_buffers()
            raise StopIteration

        # Determine the appropriate row, column index. and initialize the
        # data dictionary with empty entries.
        r, c = self.iter_inds[self.iter]
        self.iter = self.iter + 1
        data = dict([(i, None) for i in self.possible_inputs])

        # Read data from any of the input files that are defined.
        for source in self.infiles:
            data[source] = self.infiles[source].read_spectrum(r, c)
            if (self.iter % flush_rate) == 0:
                self.infiles[source].flush_buffers()

        # We apply the calibration correciton here for simplicity.
        meas = data['measured_radiance_file']
        if data["radiometry_correction_file"] is not None:
            meas = meas.copy() * data['radiometry_correction_file']

        # We build the geometry object for this spectrum.  For files not
        # specified in the input configuration block, the associated entries
        # will be 'None'. The Geometry object will use reasonable defaults.
        geom = Geometry(obs=data['obs_file'],
                        glt=data['glt_file'],
                        loc=data['loc_file'])

        # Updates are simply serialized prior distribution vectors for this
        # spectrum (or 'None' if the file was not specified in the input
        # configuration block).  The ordering is [surface, RT, instrument]
        updates = ({'prior_means': data['surface_prior_mean_file'],
                    'prior_variances': data['surface_prior_variance_file'],
                    'reflectance': data['reflectance_file']},
                   {'prior_means': data['rt_prior_mean_file'],
                    'prior_variances': data['rt_prior_variance_file']},
                   {'prior_means': data['instrument_prior_mean_file'],
                    'prior_variances': data['instrument_prior_variance_file']})

        return r, c, meas, geom, updates

    def write_spectrum(self, row, col, to_write):
        """Write data from a single inversion to all output buffers."""

        self.writes = self.writes + 1

        for product in self.outfiles:
            logging.debug('IO: Writing '+product)
            self.outfiles[product].write_spectrum(row, col, to_write[product])
            if (self.writes % flush_rate) == 0:
                self.outfiles[product].flush_buffers()

        if 'covariance_matrix_file' in self.output:
            if (self.writes & flush_rate) == 0:
                del self.mm
                self.mm = s.memmap(self.covmat, dtype='float32', mode='r+',
                    shape=(self.n_rows, self.n_cols, self.n_sv, self.n_sv))

            self.mm[row, col] = to_write['covariance_matrix_file']

        # Special case! samples file is matlab format.
        if 'mcmc_samples_file' in self.output:
            logging.debug('IO: Writing mcmc_samples_file')
            mdict = {'samples': states}
            s.io.savemat(self.output['mcmc_samples_file'], mdict)

        # Special case! Data dump file is matlab format.
        if 'data_dump_file' in self.output:
            logging.warning('data_dump_file not supported with MPI')

        # Write plots, if needed
        if 'plot_directory' in self.output:
            logging.warning('plot_directory not supported with MPI')
