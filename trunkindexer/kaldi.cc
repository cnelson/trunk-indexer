#include <Python.h>
#include "feat/wave-reader.h"
#include "online2/online-nnet3-decoding.h"
#include "online2/online-nnet2-feature-pipeline.h"
#include "online2/onlinebin-util.h"
#include "online2/online-timing.h"
#include "fstext/fstext-lib.h"
#include "lat/lattice-functions.h"
#include "util/kaldi-thread.h"
#include "nnet3/nnet-utils.h"
#include "base/kaldi-error.h"

std::string errlog;

// trap kaldi error logs so we can pass it back to python if something goes wrong
// rather than puking all over std::cerr
void trap(const kaldi::LogMessageEnvelope &envelope, const char *message) {
	using namespace kaldi;

	switch (envelope.severity) {
	    case LogMessageEnvelope::kError :
	    case LogMessageEnvelope::kAssertFailed :
		errlog += message;
		break;
	}
}

// basic kaldi implmementation mostly copied / pasted from examples
// this is more or less online2-wav-nnet3-latgen-faster
// TODO: de 10x-ify this.
std::string basic(std::string wavfile, std::string datadir) {
    using namespace kaldi;
    using namespace fst;

    typedef kaldi::int32 int32;
    typedef kaldi::int64 int64;

    SetLogHandler(trap);

    std::string word_syms_rxfilename(datadir+"/words.txt");
    std::string nnet3_rxfilename(datadir+"/final.mdl");
    std::string fst_rxfilename(datadir+"/HCLG.fst");

    std::string wav_rspecifier("scp:echo foo "+wavfile+"|");

    LatticeFasterDecoderConfig decoder_opts;
    decoder_opts.max_active = 7000;
    decoder_opts.beam = 15.0;
    decoder_opts.lattice_beam = 6.0;

    nnet3::NnetSimpleLoopedComputationOptions decodable_opts;
    decodable_opts.frame_subsampling_factor = 3;
    decodable_opts.acoustic_scale = 1.0;

    OnlineNnet2FeaturePipelineInfo feature_info;
    feature_info.feature_type = "mfcc";
    feature_info.mfcc_opts.use_energy = false;
    feature_info.mfcc_opts.num_ceps = 40;
    feature_info.mfcc_opts.mel_opts.num_bins = 40;
    feature_info.mfcc_opts.mel_opts.low_freq = 40;
    feature_info.mfcc_opts.mel_opts.high_freq = -200;
    feature_info.mfcc_opts.frame_opts.samp_freq = 8000;

    feature_info.use_ivectors = true;
    feature_info.ivector_extractor_info.ivector_period = 10;
    feature_info.ivector_extractor_info.num_cg_iters = 15;
    feature_info.ivector_extractor_info.num_gselect = 5;
    feature_info.ivector_extractor_info.min_post = 0.025;
    feature_info.ivector_extractor_info.posterior_scale = 0.1;
    feature_info.ivector_extractor_info.max_remembered_frames= 1000;
    feature_info.ivector_extractor_info.max_count = 100;
    feature_info.ivector_extractor_info.use_most_recent_ivector = true;
    feature_info.ivector_extractor_info.greedy_ivector_extractor = true;
    feature_info.ivector_extractor_info.splice_opts.left_context = 3;
    feature_info.ivector_extractor_info.splice_opts.right_context = 3;
    ReadKaldiObject(datadir+"/final.mat", &feature_info.ivector_extractor_info.lda_mat);
    ReadKaldiObject(datadir+"/global_cmvn.stats", &feature_info.ivector_extractor_info.global_cmvn_stats);
    ReadKaldiObject(datadir+"/final.dubm", &feature_info.ivector_extractor_info.diag_ubm);
    ReadKaldiObject(datadir+"/final.ie", &feature_info.ivector_extractor_info.extractor);

    TransitionModel trans_model;
    nnet3::AmNnetSimple am_nnet;
    {
      bool binary;
      Input ki(nnet3_rxfilename, &binary);
      trans_model.Read(ki.Stream(), binary);
      am_nnet.Read(ki.Stream(), binary);
      SetBatchnormTestMode(true, &(am_nnet.GetNnet()));
      SetDropoutTestMode(true, &(am_nnet.GetNnet()));
      nnet3::CollapseModel(nnet3::CollapseModelConfig(), &(am_nnet.GetNnet()));
    }

    nnet3::DecodableNnetSimpleLoopedInfo decodable_info(decodable_opts,
                                                        &am_nnet);

    fst::Fst<fst::StdArc> *decode_fst = ReadFstKaldiGeneric(fst_rxfilename);

    fst::SymbolTable *word_syms = NULL;
    if (word_syms_rxfilename != "")
      if (!(word_syms = fst::SymbolTable::ReadText(word_syms_rxfilename)))
        KALDI_ERR << "Could not read symbol table from file "
                  << word_syms_rxfilename;

    RandomAccessTableReader<WaveHolder> wav_reader(wav_rspecifier);

    OnlineTimingStats timing_stats;

    OnlineIvectorExtractorAdaptationState adaptation_state(
          feature_info.ivector_extractor_info);
    const WaveData &wave_data = wav_reader.Value("foo");
    SubVector<BaseFloat> data(wave_data.Data(), 0);

    OnlineNnet2FeaturePipeline feature_pipeline(feature_info);
    feature_pipeline.SetAdaptationState(adaptation_state);

    SingleUtteranceNnet3Decoder decoder(decoder_opts, trans_model,
                                            decodable_info,
                                            *decode_fst, &feature_pipeline);
    OnlineTimer decoding_timer("foo");

    BaseFloat samp_freq = wave_data.SampFreq();
    int32 chunk_length;
    chunk_length = std::numeric_limits<int32>::max();

    int32 samp_offset = 0;
    std::vector<std::pair<int32, BaseFloat> > delta_weights;

    while (samp_offset < data.Dim()) {
        int32 samp_remaining = data.Dim() - samp_offset;
        int32 num_samp = chunk_length < samp_remaining ? chunk_length
                                                         : samp_remaining;

        SubVector<BaseFloat> wave_part(data, samp_offset, num_samp);
        feature_pipeline.AcceptWaveform(samp_freq, wave_part);

        samp_offset += num_samp;
        decoding_timer.WaitUntil(samp_offset / samp_freq);
        if (samp_offset == data.Dim()) {
            // no more input. flush out last frames
            feature_pipeline.InputFinished();
        }

        decoder.AdvanceDecoding();
    }
    decoder.FinalizeDecoding();

    CompactLattice clat;
    decoder.GetLattice(true, &clat);

    CompactLattice best_path_clat;
    CompactLatticeShortestPath(clat, &best_path_clat);

    Lattice best_path_lat;
    ConvertLattice(best_path_clat, &best_path_lat);

    LatticeWeight weight;
    std::vector<int32> alignment;
    std::vector<int32> words;

    GetLinearSymbolSequence(best_path_lat, &alignment, &words, &weight);

    std::string result;
    for (size_t i = 0; i < words.size(); i++) {
        std::string s = word_syms->Find(words[i]);
        if (s == "")
            KALDI_ERR << "Word-id " << words[i] << " not in symbol table.";
        result += s + ' ';
    }

    delete decode_fst;
    delete word_syms; // will delete if non-NULL.
    return result;
}

static PyObject * decode(PyObject * self, PyObject * args)
{
    char * wavfile;
    char * datadir;

    if (!PyArg_ParseTuple(args, "ss", &wavfile, &datadir)) {
        return NULL;
    }

    std::string w(wavfile);
    std::string d(datadir);
    std::string result;

    try {
        result = basic(w, d);
        return PyUnicode_FromString(result.c_str());
    } catch(const std::exception& e) {
        std::cerr << e.what();
        PyErr_SetString(PyExc_RuntimeError, errlog.c_str());
        return NULL;
   }
}

static PyMethodDef Methods[] = {
    { "decode", decode, METH_VARARGS, "Decode an 8khz wav file to text" },
    { NULL, NULL, 0, NULL }
};

static struct PyModuleDef module = {
    PyModuleDef_HEAD_INIT,
    "trunkindexer.kaldi",
    NULL,
    -1,
    Methods
};

PyMODINIT_FUNC
PyInit_kaldi(void)
{
    return PyModule_Create(&module);
}

