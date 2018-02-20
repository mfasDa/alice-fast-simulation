/*************************************************************************
 * Copyright(c) 1998-2016, ALICE Experiment at CERN, All rights reserved. *
 *                                                                        *
 * Author: The ALICE Off-line Project.                                    *
 * Contributors are mentioned in the code where appropriate.              *
 *                                                                        *
 * Permission to use, copy, modify and distribute this software and its   *
 * documentation strictly for non-commercial purposes is hereby granted   *
 * without fee, provided that the above copyright notice appears in all   *
 * copies and that both the copyright notice and this permission notice   *
 * appear in the supporting documentation. The authors make no claims     *
 * about the suitability of this software for any purpose. It is          *
 * provided "as is" without express or implied warranty.                  *
 **************************************************************************/

#include <iostream>

#include <TParticle.h>
#include <TTree.h>
#include <TFile.h>

#include <AliPythiaRndm.h>
#include <AliDecayerPythia.h>
#include <AliLog.h>
#include <AliRun.h>
#include <AliGenPythiaEventHeader.h>

#include "AliPythiaBase_dev.h"

#include "AliGenPythia_dev.h"

/// \cond CLASSIMP
ClassImp(AliGenPythia_dev);
/// \endcond

/**
 *  Default Constructor
 */
AliGenPythia_dev::AliGenPythia_dev():
  AliGenMC(),
  fPythia(nullptr),
  fProcess(kPyCharm),
  fItune(kUnknownTune),
  fStrucFunc(kCTEQ5L),
  fWeightPower(0.),
  fPtHardMin(0.),
  fPtHardMax(1.e4),
  fYHardMin(-1.e10),
  fYHardMax(1.e10),
  fGinit(1),
  fGfinal(1),
  fCRoff(0),
  fSetNuclei(kFALSE),
  fUseNuclearPDF(kFALSE),
  fUseLorentzBoost(kTRUE),
  fNewMIS(kFALSE),
  fNucPdf(0),
  fLHEFile(),
  fTriggerParticlePDG(-1),
  fTriggerParticleMinPt(-1),
  fTriggerParticleMaxPt(-1),
  fTriggerParticleMinEta(-1),
  fTriggerParticleMaxEta(-1),
  fNev(0),
  fXsection(0.),
  fTrials(0),
  fTrialsRun(0),
  fDecayer(new AliDecayerPythia()),
  fDebugEventFirst(-1),
  fDebugEventLast(-1),
  fHeader(0),
  fNParent()
{
  if (!AliPythiaRndm::GetPythiaRandom()) AliPythiaRndm::SetPythiaRandom(GetRandom());
}

/**
 *  Standard Constructor
 */
AliGenPythia_dev::AliGenPythia_dev(AliPythiaBase_dev* pythia):
  AliGenMC(),
  fPythia(pythia),
  fProcess(kPyCharm),
  fItune(kUnknownTune),
  fStrucFunc(kCTEQ5L),
  fWeightPower(0.),
  fPtHardMin(0.),
  fPtHardMax(1.e4),
  fYHardMin(-1.e10),
  fYHardMax(1.e10),
  fGinit(1),
  fGfinal(1),
  fCRoff(0),
  fSetNuclei(kFALSE),
  fUseNuclearPDF(kFALSE),
  fUseLorentzBoost(kTRUE),
  fNewMIS(kFALSE),
  fNucPdf(0),
  fLHEFile(),
  fTriggerParticlePDG(-1),
  fTriggerParticleMinPt(-1),
  fTriggerParticleMaxPt(-1),
  fTriggerParticleMinEta(-1),
  fTriggerParticleMaxEta(-1),
  fNev(0),
  fXsection(0.),
  fTrials(0),
  fTrialsRun(0),
  fDecayer(new AliDecayerPythia()),
  fDebugEventFirst(-1),
  fDebugEventLast(-1),
  fHeader(0),
  fNParent()
{
  if (!AliPythiaRndm::GetPythiaRandom()) AliPythiaRndm::SetPythiaRandom(GetRandom());
}

/**
 * Set a range of event numbers, for which a table
 * of generated particle will be printed
 * @param eventFirst First event number to be printed
 * @param eventLast  Last event number to be printed
 */
void AliGenPythia_dev::SetEventListRange(Int_t eventFirst, Int_t eventLast)
{
  fDebugEventFirst = eventFirst;
  fDebugEventLast  = eventLast;
  if (fDebugEventLast==-1) fDebugEventLast=fDebugEventFirst;
}

/**
 * Initialization
 */
void AliGenPythia_dev::Init()
{
  if (!fPythia) {
    AliErrorStream() << "No PYTHIA generator found! Initialization not done!" << std::endl;
    return;
  }

  if (fWeightPower != 0) fPythia->SetWeightPower(fWeightPower);
  fPythia->SetPtHardRange(fPtHardMin, fPtHardMax);
  fPythia->SetYHardRange(fYHardMin, fYHardMax);

  // Initial/final state radiation
  fPythia->SetInitialAndFinalStateRadiation(fGinit, fGfinal);

  // Nuclei
  if (fAProjectile > 0 && fATarget > 0 && fUseNuclearPDF) fPythia->SetNuclei(fAProjectile, fATarget, fNucPdf);

  fPythia->SetLHEFile(fLHEFile);

  fPythia->ProcInit(fProcess, fEnergyCMS, fStrucFunc, fItune);
  //  Forward Paramters to the AliPythia object
  fDecayer->SetForceDecay(fForceDecay);

  fDecayer->Init();

  //  This counts the total number of calls to Pyevnt() per run.
  fTrialsRun = 0;
  fNev       = 0 ;

  AliGenMC::Init();

  // Reset Lorentz boost if requested
  if (!fUseLorentzBoost) {
    fDyBoost = 0;
    AliWarningStream() << "Demand to discard Lorentz boost." << std::endl;
  }
}

/**
 * Generate one event
 */
void AliGenPythia_dev::Generate()
{
  if (!fPythia) {
    AliErrorStream() << "No PYTHIA generator found! No event generated!" << std::endl;
    return;
  }

  fDecayer->ForceDecay();

  Double_t polar[3]   = {0};
  Double_t origin[3]  = {0};
  Double_t p[4]       = {0};

  //  converts from mm/c to s
  const Float_t kconv = 0.001 / TMath::C();
  //
  Int_t nt = 0;
  Int_t jev = 0;
  Int_t j = 0, kf = 0;
  fTrials = 0;

  //  Set collision vertex position
  if (fVertexSmear == kPerEvent) Vertex();

  //  event loop
  do
  {
    // Produce new event
    fPythia->GenerateEvent();

    fTrials++;
    fPythia->GetParticles(&fParticles);

    Int_t np = fParticles.GetEntriesFast();
    if (np == 0) continue;

    if (TMath::Abs(fDyBoost) > 1.e-4) Boost();
    if (TMath::Abs(fXingAngleY) > 1.e-10) BeamCrossAngle();

    fNprimaries = DoGenerate();
    fTrialsRun += fTrials;
    fNev++;
    MakeHeader();
    break;

  } while (true); // event loop

  SetHighWaterMark(nt);

  //  get cross-section
  fXsection = fPythia->GetXSection();
}

Bool_t AliGenPythia_dev::IsFromHeavyFlavor(const TParticle* part) const
{
  // Check if this is a heavy flavor decay product
  Int_t mfl = GetFlavor(TMath::Abs(part->GetPdgCode()));

  // Heavy flavor hadron
  if (mfl >= 4 && mfl < 6) return kTRUE;

  // Light hadron
  Int_t imo = part->GetFirstMother() - 1;
  const TParticle* pm = part;

  // Heavy flavor hadron produced by generator
  while (imo >  -1) {
    pm  =  static_cast<TParticle*>(fParticles.At(imo));
    Int_t mpdg = TMath::Abs(pm->GetPdgCode());
    mfl  = GetFlavor(mpdg);
    if ((mfl > 3) && (mfl < 6) && mpdg > 400) return kTRUE;
    imo = pm->GetFirstMother() - 1;
  }
  return kFALSE;
}

Int_t AliGenPythia_dev::DoGenerate()
{
  Int_t nc = 0;
  Double_t p[4];
  Double_t polar[3]   =   {0,0,0};
  Double_t origin[3]  =   {0,0,0};
  //  converts from mm/c to s
  const Float_t kconv = 0.001 / TMath::C();


  Int_t np = fParticles.GetEntriesFast();

  fNParent.clear();
  fNParent.resize(np, 0);

  Bool_t triggered = kTRUE;
  if (fTriggerParticlePDG >= 0) {
    Bool_t triggered = kFALSE;
    for (Int_t i = 0; i < np; i++) {
      TParticle*  iparticle = static_cast<TParticle*>(fParticles.At(i));
      if (fTriggerParticlePDG != 0) {
        Int_t kf = CheckPDGCode(iparticle->GetPdgCode());
        if (kf != fTriggerParticlePDG) continue;
      }
      if (fTriggerParticleMinPt < fTriggerParticleMaxPt && fTriggerParticleMinPt >= 0 && (iparticle->Pt() < fTriggerParticleMinPt || iparticle->Pt() > fTriggerParticleMaxPt)) continue;
      if (fTriggerParticleMinEta < fTriggerParticleMaxEta && fTriggerParticleMinEta >= 0 && (iparticle->Eta() < fTriggerParticleMinEta || iparticle->Eta() > fTriggerParticleMaxEta)) continue;
      triggered = kTRUE;
      break;
    }
    if (!triggered) return 0;
  }

  for (Int_t i = 0; i < np; i++) {
    Int_t trackIt = 0;
    TParticle *  iparticle = static_cast<TParticle*>(fParticles.At(i));
    Int_t kf = CheckPDGCode(iparticle->GetPdgCode());
    Int_t ks = iparticle->GetStatusCode();
    Int_t km = iparticle->GetFirstMother();
    if (
        (((ks == 1  && kf!=0 && KinematicSelection(iparticle, 0)) || (ks !=1)) && IsFromHeavyFlavor(iparticle)) ||
        ((fProcess == kPyJets || fProcess == kPyJetsPWHG || fProcess == kPyCharmPWHG || fProcess == kPyBeautyPWHG) && ks == 21 && km == 0 && i > 1)
    )
    {
      nc++;
      if (ks == 1) trackIt = 1;
      Int_t ipa = iparticle->GetFirstMother() - 1;

      Int_t iparent = (ipa > -1) ? fNParent[ipa] : -1;

      // store track information
      p[0] = iparticle->Px();
      p[1] = iparticle->Py();
      p[2] = iparticle->Pz();
      p[3] = iparticle->Energy();

      origin[0] = fVertex[0]+iparticle->Vx()/10; // [cm]
      origin[1] = fVertex[1]+iparticle->Vy()/10; // [cm]
      origin[2] = fVertex[2]+iparticle->Vz()/10; // [cm]

      Float_t tof = fTime + kconv * iparticle->T();

      Int_t nt = -1;
      PushTrack(fTrackIt * trackIt, iparent, kf,
          p[0], p[1], p[2], p[3],
          origin[0], origin[1], origin[2], tof,
          polar[0], polar[1], polar[2],
          kPPrimary, nt, 1., ks);
      nc++;
      KeepTrack(nt);
      fNParent[i] = nt;
      SetHighWaterMark(nt);

    } // select particle
  } // particle loop

  return nc;
}

/**
 * Print x-section summary
 */
void AliGenPythia_dev::FinishRun()
{
  fPythia->PrintStatistics();

  AliInfoStream() << "Total number of Pyevnt() calls " << fTrialsRun << std::endl;

  WriteXsection();
}

/**
 * Treat protons as inside nuclei with mass numbers a1 and a2
 */
void AliGenPythia_dev::SetNuclei(Int_t a1, Int_t a2, Int_t pdfset)
{
  fAProjectile   = a1;
  fATarget       = a2;
  fNucPdf        = pdfset;  // 0 EKS98 9 EPS09LO 19 EPS09NLO
  fUseNuclearPDF = kTRUE;
  fSetNuclei     = kTRUE;
}

void AliGenPythia_dev::SetTriggerParticle(Int_t pdg, Float_t minPt, Float_t maxPt, Float_t minEta, Float_t maxEta)
{
  fTriggerParticlePDG = pdg;
  fTriggerParticleMinPt = minPt;
  fTriggerParticleMaxPt = maxPt;
  fTriggerParticleMinEta = minEta;
  fTriggerParticleMaxEta = maxEta;
}

/**
 * Make header for the simulated event
 */
void AliGenPythia_dev::MakeHeader()
{
  if (gAlice && gAlice->GetEvNumber()>=fDebugEventFirst && gAlice->GetEvNumber()<=fDebugEventLast) fPythia->EventListing();

  // Builds the event header, to be called after each event
  if (fHeader) delete fHeader;
  AliGenPythiaEventHeader* pythiaHeader = new AliGenPythiaEventHeader("Pythia");
  fHeader = pythiaHeader;
  fHeader->SetTitle(GetTitle());

  pythiaHeader->SetProcessType(fPythia->ProcessCode());

  // Number of trials
  pythiaHeader->SetTrials(fTrials);

  // Event Vertex
  fHeader->SetPrimaryVertex(fVertex);
  fHeader->SetInteractionTime(fTime);

  // Number of primaries
  fHeader->SetNProduced(fNprimaries);

  // Event weight
  fHeader->SetEventWeight(fPythia->GetEventWeight());

  // Number of MPI
  fHeader->SetNMPI(fPythia->GetNMPI());

  // Store pt^hard and cross-section
  pythiaHeader->SetPtHard(fPythia->GetPtHard());
  pythiaHeader->SetXsection(fPythia->GetXSection());

  //  Pass header
  AddHeader(fHeader);
  fHeader = nullptr;
}

/**
 *  Write cross section and Ntrials to a tree in a file
 *  Used for pt-hard bin productions
 */
void AliGenPythia_dev::WriteXsection(const Char_t *fname) const
{
  TFile fout(fname,"recreate");
  TTree tree("Xsection","Pythia cross section");
  // Convert to expected types for backwards compatibility
  Double_t xsec = fXsection;
  UInt_t trials = fTrialsRun;
  tree.Branch("xsection", &xsec, "X/D");
  tree.Branch("ntrials" , &trials , "X/i");
  tree.Fill();
  tree.Write();
  fout.Close();
}