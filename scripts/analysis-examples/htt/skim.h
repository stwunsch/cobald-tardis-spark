#ifndef SKIM_H
#define SKIM_H

#include "ROOT/RDataFrame.hxx"
#include "ROOT/RVec.hxx"
#include <vector>

using namespace ROOT::VecOps;

namespace Helper {
template <typename T> float DeltaPhi(T v1, T v2, const T c = M_PI) {
  auto r = std::fmod(v2 - v1, 2.0 * c);
  if (r < -c) {
    r += 2.0 * c;
  } else if (r > c) {
    r -= 2.0 * c;
  }
  return r;
}
} // namespace Helper

std::vector<int> build_pair(RVec<int> &goodMuons, RVec<float> &pt_1,
                            RVec<float> &eta_1, RVec<float> &phi_1,
                            RVec<int> &goodTaus, RVec<float> &iso_2,
                            RVec<float> &eta_2, RVec<float> &phi_2) {
  // Get indices of all possible combinations
  auto comb = Combinations(pt_1, eta_2);
  const auto numComb = comb[0].size();

  // Find valid pairs based on delta r
  std::vector<int> validPair(numComb, 0);
  for (size_t i = 0; i < numComb; i++) {
    const auto i1 = comb[0][i];
    const auto i2 = comb[1][i];
    if (goodMuons[i1] == 1 && goodTaus[i2] == 1) {
      const auto deltar = sqrt(pow(eta_1[i1] - eta_2[i2], 2) +
                               pow(Helper::DeltaPhi(phi_1[i1], phi_2[i2]), 2));
      if (deltar > 0.5) {
        validPair[i] = 1;
      }
    }
  }

  // Find best muon based on pt
  int idx_1 = -1;
  float maxPt = -1;
  for (size_t i = 0; i < numComb; i++) {
    if (validPair[i] == 0)
      continue;
    const auto tmp = comb[0][i];
    if (maxPt < pt_1[tmp]) {
      maxPt = pt_1[tmp];
      idx_1 = tmp;
    }
  }

  // Find best tau based on iso
  int idx_2 = -1;
  float minIso = 999;
  for (size_t i = 0; i < numComb; i++) {
    if (validPair[i] == 0)
      continue;
    if (int(comb[0][i]) != idx_1)
      continue;
    const auto tmp = comb[1][i];
    if (minIso > iso_2[tmp]) {
      minIso = iso_2[tmp];
      idx_2 = tmp;
    }
  }

  return std::vector<int>({idx_1, idx_2});
}

ROOT::Math::PtEtaPhiMVector add_p4(float pt, float eta, float phi, float mass)
{
    return ROOT::Math::PtEtaPhiMVector(pt, eta, phi, mass);
}

float get_first(RVec<float> &x, RVec<int>& g)
{
    if (Sum(g) >= 1) return x[g][0];
    return -999.f;
}

float get_second(RVec<float> &x, RVec<int>& g)
{
    if (Sum(g) >= 2) return x[g][1];
    return -999.f;
}

float compute_mjj(ROOT::Math::PtEtaPhiMVector& p4, RVec<int>& g)
{
    if (Sum(g) >= 2) return float(p4.M());
    return -999.f;
}

float compute_ptjj(ROOT::Math::PtEtaPhiMVector& p4, RVec<int>& g)
{
    if (Sum(g) >= 2) return float(p4.Pt());
    return -999.f;
}

float compute_jdeta(float x, float y, RVec<int>& g)
{
    if (Sum(g) >= 2) return x - y;
    return -999.f;
}

float compute_mt(float pt_1, float phi_1, float pt_met, float phi_met)
{
    const auto dphi = Helper::DeltaPhi(phi_1, phi_met);
    return std::sqrt(2.0 * pt_1 * pt_met * (1.0 - std::cos(dphi)));
}

#endif
