import datetime
import os

import pyspark
import ROOT

ROOT.gROOT.SetBatch(True)
RDataFrame = ROOT.RDF.Experimental.Distributed.Spark.RDataFrame

###############################################################################
# SKIMMING FUNCTIONS
###############################################################################

# Names of the datasets to be found in the base path and processed for the
# analysis
samplesandlabels = [
    ("GluGluToHToTauTau", "ggH"),
    ("VBF_HToTauTau", "qqH"),
    ("W1JetsToLNu", "W1J"),
    ("W2JetsToLNu", "W2J"),
    ("W3JetsToLNu", "W3J"),
    ("TTbar", "TT"),
    ("DYJetsToLL", "ZLL"),
    ("DYJetsToLL", "ZTT"),
    ("Run2012B_TauPlusX", "dataRunB"),
    ("Run2012C_TauPlusX", "dataRunC"),
]

# Compute event weights to be used for the respective datasets
# The event weight reweights the full dataset so that the sum of the weights
# is equal to the expected number of events in data. The expectation is given
# by multiplying the integrated luminosity of the data with the cross-section
# of the process in the datasets divided by the number of simulated events.
integratedLuminosity = 11.467 * 1000.0  # Run2012B+C
eventWeights = {
    "GluGluToHToTauTau": 19.6 / 476963.0 * integratedLuminosity,
    "VBF_HToTauTau": 1.55 / 491653.0 * integratedLuminosity,
    "DYJetsToLL": 3503.7 / 30458871.0 * integratedLuminosity,
    "TTbar": 225.2 / 6423106.0 * integratedLuminosity,
    "W1JetsToLNu": 6381.2 / 29784800.0 * integratedLuminosity,
    "W2JetsToLNu": 2039.8 / 30693853.0 * integratedLuminosity,
    "W3JetsToLNu": 612.5 / 15241144.0 * integratedLuminosity,
    "Run2012B_TauPlusX": 1.0,
    "Run2012C_TauPlusX": 1.0,
}


def MinimalSelection(df):
    """Perform a selection on the minimal requirements of an event."""
    return df.Filter("HLT_IsoMu17_eta2p1_LooseIsoPFTau20 == true",
                     "Passes trigger")\
        .Filter("nMuon > 0", "nMuon > 0")\
        .Filter("nTau > 0", "nTau > 0")


def FindGoodMuons(df):
    """Find the interesting muons in the muon collection."""
    return df.Define(
        "goodMuons",
        "abs(Muon_eta) < 2.1 && Muon_pt > 17 && Muon_tightId == true")


def FindGoodTaus(df):
    """
    Find the interesting taus in the tau collection. The tau candidates in this
    collection represent hadronic decays of taus, which means that the tau
    decays to combinations of pions and neutrinos in the final state.
    """
    return df.Define("goodTaus",
                     "Tau_charge != 0 && abs(Tau_eta) < 2.3 && Tau_pt > 20 && \
                      Tau_idDecayMode == true && Tau_idIsoTight == true && \
                      Tau_idAntiEleTight == true && Tau_idAntiMuTight == true")


def FilterGoodEvents(df):
    """
    Reduce the dataset to the interesting events containing at least one
    interesting muon and tau candidate.
    """
    return df.Filter("Sum(goodTaus) > 0", "Event has good taus")\
             .Filter("Sum(goodMuons) > 0", "Event has good muons")


def FindMuonTauPair(df):
    """
    Select a muon-tau pair from the collections of muons and taus passing the
    initial selection. The selected pair represents the candidate for this
    event for a Higgs boson decay to two tau leptons of which one decays to a
    hadronic final state (most likely a combination of pions) and one decays to
    a muon and a neutrino.
    """

    return df.Define("pairIdx",
                     "build_pair(goodMuons, Muon_pt, Muon_eta, Muon_phi,\
                                 goodTaus, Tau_relIso_all, Tau_eta, Tau_phi)")\
        .Define("idx_1", "pairIdx[0]")\
        .Define("idx_2", "pairIdx[1]")\
        .Filter("idx_1 != -1", "Valid muon in selected pair")\
        .Filter("idx_2 != -1", "Valid tau in selected pair")


def DeclareVariables(df):
    """Declare variables to be studied in the analysis."""

    variables = {
        "pt_1": "Muon_pt[idx_1]",
        "eta_1": "Muon_eta[idx_1]",
                 "phi_1": "Muon_phi[idx_1]",
                 "m_1": "Muon_mass[idx_1]",
                 "iso_1": "Muon_pfRelIso03_all[idx_1]",
                 "q_1": "Muon_charge[idx_1]",
                 "pt_2": "Tau_pt[idx_2]",
                 "eta_2": "Tau_eta[idx_2]",
                 "phi_2": "Tau_phi[idx_2]",
                 "m_2": "Tau_mass[idx_2]",
                 "iso_2": "Tau_relIso_all[idx_2]",
                 "q_2": "Tau_charge[idx_2]",
                 "dm_2": "Tau_decayMode[idx_2]",
                 "pt_met": "MET_pt",
                 "phi_met": "MET_phi",
                 "p4_1": "add_p4(pt_1, eta_1, phi_1, m_1)",
                 "p4_2": "add_p4(pt_2, eta_2, phi_2, m_2)",
                 "p4": "p4_1 + p4_2",
                 "mt_1": "compute_mt(pt_1, phi_1, pt_met, phi_met)",
                 "mt_2": "compute_mt(pt_2, phi_2, pt_met, phi_met)",
                 "m_vis": "float(p4.M())",
                 "pt_vis": "float(p4.Pt())",
                 "npv": "PV_npvs",
                 "goodJets": "Jet_puId == true\
                              && abs(Jet_eta) < 4.7\
                              && Jet_pt > 30",
                 "njets": "Sum(goodJets)",
                 "jpt_1": "get_first(Jet_pt, goodJets)",
                 "jeta_1": "get_first(Jet_eta, goodJets)",
                 "jphi_1": "get_first(Jet_phi, goodJets)",
                 "jm_1": "get_first(Jet_mass, goodJets)",
                 "jbtag_1": "get_first(Jet_btag, goodJets)",
                 "jpt_2": "get_second(Jet_pt, goodJets)",
                 "jeta_2": "get_second(Jet_eta, goodJets)",
                 "jphi_2": "get_second(Jet_phi, goodJets)",
                 "jm_2": "get_second(Jet_mass, goodJets)",
                 "jbtag_2": "get_second(Jet_btag, goodJets)",
                 "jp4_1": "add_p4(jpt_1, jeta_1, jphi_1, jm_1)",
                 "jp4_2": "add_p4(jpt_2, jeta_2, jphi_2, jm_2)",
                 "jp4": "jp4_1 + jp4_2",
                 "mjj": "compute_mjj(jp4, goodJets)",
                 "ptjj": "compute_ptjj(jp4, goodJets)",
                 "jdeta": "compute_jdeta(jeta_1, jeta_2, goodJets)"
    }

    for var, op in variables.items():
        df = df.Define(var, op)

    return df


def AddEventWeight(df, sample):
    """ Add the event weight to the dataset as the column `weight`"""
    weight = eventWeights[sample]
    return df.Define("weight", "{}".format(weight))


def CheckGeneratorTaus(df, sample):
    """
    Check that the generator particles matched to the reconstructed taus are
    actually taus and add this information the the dataset. This information is
    used to estimate the fraction of events that are falsely reconstructed as
    taus, e.g., electrons or jets that could fake such a particle.
    """
    if "Run2012" in sample:
        return df.Define("gen_match", "false")
    else:
        return df.Define("gen_match",
                         "abs(GenPart_pdgId[Muon_genPartIdx[idx_1]]) == 15 && \
                          abs(GenPart_pdgId[Tau_genPartIdx[idx_2]]) == 15")


def skim(df, sample):
    """
    Main function of the skimming step of the analysis
    The function loops over all required samples, reduces the content to the
    interesting events and writes them to new files.
    """

    df2 = MinimalSelection(df)
    df3 = FindGoodMuons(df2)
    df4 = FindGoodTaus(df3)
    df5 = FilterGoodEvents(df4)
    df6 = FindMuonTauPair(df5)
    df7 = DeclareVariables(df6)
    df8 = CheckGeneratorTaus(df7, sample)
    df9 = AddEventWeight(df8, sample)

    return df9


# Declare all variables which shall end up in the final reduced dataset
final_variables_list = [
    "njets", "npv",
    "pt_1", "eta_1", "phi_1", "m_1", "iso_1", "q_1", "mt_1",
    "pt_2", "eta_2", "phi_2", "m_2", "iso_2", "q_2", "mt_2", "dm_2",
    "jpt_1", "jeta_1", "jphi_1", "jm_1", "jbtag_1",
    "jpt_2", "jeta_2", "jphi_2", "jm_2", "jbtag_2",
    "pt_met", "phi_met", "m_vis", "pt_vis", "mjj", "ptjj", "jdeta",
    "gen_match", "run", "weight"
]


def main():
    print("Starting HTT skimming")
    skimwatch = ROOT.TStopwatch()
    # Example configuration with a YARN cluster, change according to needs
    conf = {
        "spark.master": "yarn",
        "spark.executorEnv.LD_LIBRARY_PATH": os.environ["LD_LIBRARY_PATH"],
        "spark.yarn.queue": "htt_skim",
    }
    sconf = pyspark.SparkConf().setAll(conf.items())
    sc = pyspark.SparkContext(conf=sconf)

    # Define correct basepath for input data and output snapshotted files
    inbasepath = ("root://eospublic.cern.ch/"
                  "/eos/root-eos/benchmark/CMSOpenDataHiggsTauTau/")
    outbasepath = "/work/vpadulano/distrdf/htt-tests/skimdata/partials/"

    for sample, label in samplesandlabels:
        print("\tStarting skimming sample {} with label {}".format(sample, label))
        skimwatch_inner = ROOT.TStopwatch()

        filenames = [
            inbasepath + sample + "_{}.root".format(i)
            for i in range(1, 2)  # Choose how many files to use per sample
        ]
        # Create RDataFrame
        # optional argument `npartitions` to select the number of partitions
        df = RDataFrame("Events", filenames)
        # Distribute the skim.h header to the workers
        df._headnode.backend.distribute_headers("skim.h")

        # Book events skimming
        skimdf = skim(df, sample)
        # Snapshot needed columns
        out_file = outbasepath + sample + "_skim.root"
        skimdf.Snapshot("Events", out_file, final_variables_list)

        skimwatch_inner.Stop()
        sampletime = round(skimwatch_inner.RealTime(), 2)
        print("\tSample skimming ended in {} hh:mm:ss".format(datetime.timedelta(seconds=int(sampletime))))

    skimwatch.Stop()
    skimtime = round(skimwatch.RealTime(), 2)
    print("HTT skimming ended in {} hh:mm:ss".format(datetime.timedelta(seconds=int(sampletime))))


if __name__ == "__main__":
    main()
