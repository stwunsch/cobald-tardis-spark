import argparse
import datetime
import os

import pyspark
import ROOT

ROOT.gROOT.SetBatch(True)
RDataFrame = ROOT.RDF.Experimental.Distributed.Spark.RDataFrame

parser = argparse.ArgumentParser()
parser.add_argument("--out", help="Name of the csv where the execution time of the analysis should be written.")
args = parser.parse_args()


def dimuonSpectrum(df):

    # Select events with exactly two muons
    df_2mu = df.Filter("nMuon == 2", "Events with exactly two muons")

    # Select events with two muons of opposite charge
    df_os = df_2mu.Filter(
        "Muon_charge[0] != Muon_charge[1]", "Muons with opposite charge")

    # Compute invariant mass of the dimuon system
    df_mass = df_os.Define(
        "Dimuon_mass",
        "ROOT::VecOps::InvariantMass(Muon_pt, Muon_eta, Muon_phi, Muon_mass)")

    # Book histogram of dimuon mass spectrum
    bins = 30000  # Number of bins in the histogram
    low = 0.25  # Lower edge of the histogram
    up = 300.0  # Upper edge of the histogram

    hist = df_mass.Histo1D(ROOT.RDF.TH1DModel("", "", bins, low, up), "Dimuon_mass")

    # Create canvas for plotting
    ROOT.gStyle.SetOptStat(0)
    ROOT.gStyle.SetTextFont(42)
    c = ROOT.TCanvas("c", "", 800, 700)
    c.SetLogx()
    c.SetLogy()

    # Draw histogram
    hist.GetXaxis().SetTitle("m_{#mu#mu} (GeV)")
    hist.GetXaxis().SetTitleSize(0.04)
    hist.GetYaxis().SetTitle("N_{Events}")
    hist.GetYaxis().SetTitleSize(0.04)
    hist.SetStats(False)
    hist.Draw()

    # Draw labels
    label = ROOT.TLatex()
    label.SetTextAlign(22)
    label.DrawLatex(0.55, 3.0e4, "#eta")
    label.DrawLatex(0.77, 7.0e4, "#rho,#omega")
    label.DrawLatex(1.20, 4.0e4, "#phi")
    label.DrawLatex(4.40, 1.0e5, "J/#psi")
    label.DrawLatex(4.60, 1.0e4, "#psi'")
    label.DrawLatex(12.0, 2.0e4, "Y(1,2,3S)")
    label.DrawLatex(91.0, 1.5e4, "Z")
    label.SetNDC(True)
    label.SetTextAlign(11)
    label.SetTextSize(0.04)
    label.DrawLatex(0.10, 0.92, "#bf{CMS Open Data}")
    label.SetTextAlign(31)
    label.DrawLatex(0.90, 0.92, "#sqrt{s} = 8 TeV, L_{int} = 11.6 fb^{-1}")

    # Save Canvas to image
    c.SaveAs("dimuonSpectrum.png")


filenames = [("root://eospublic.cern.ch/"
              "/eos/root-eos/benchmark/CMSOpenDataDimuon/"
              "Run2012BC_DoubleMuParked_Muons_{}.root").format(i)
             for i in range(1, 2)  # Choose how many files to run with
             ]

# Example configuration with a YARN cluster, change according to needs
conf = {
    "spark.master": "yarn",
    "spark.executorEnv.LD_LIBRARY_PATH": os.environ["LD_LIBRARY_PATH"],
    "spark.yarn.queue": "dimuon"
}

sconf = pyspark.SparkConf().setAll(conf.items())
sc = pyspark.SparkContext(conf=sconf)

# Create the dataframe with the sparkcontext and number of partitions
df = RDataFrame("Events", filenames, sparkcontext=sc, npartitions=100)

start_time = datetime.datetime.now()
filename = f"test1_{start_time.year}_{start_time.month}_{start_time.day}_{start_time.hour}_{start_time.minute}.csv"
outcsv = args.out if args.out else filename

# Run the analysis
print("Starting dimuon analysis")
dimuonwatch = ROOT.TStopwatch()
dimuonSpectrum(df)
dimuonwatch.Stop()
dimuontime = round(dimuonwatch.RealTime(), 2)
print("\tDimuon analysis ended in {} hh:mm:ss".format(datetime.timedelta(seconds=int(dimuontime))))

# Save runtime to csv file
with open(outcsv, "a+") as f:
    f.write(str(dimuontime))
    f.write("\n")
