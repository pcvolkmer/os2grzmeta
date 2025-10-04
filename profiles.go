package main

import (
	_ "embed"
	"encoding/json"
)

type Klinik struct {
	Ik       string    `json:"ik"`
	Name     string    `json:"name"`
	Grz      []string  `json:"grz"`
	Kdk      []string  `json:"kdk"`
	Profiles []Profile `json:"profiles"`
}

type Profile struct {
	Name                          string `json:"name"`
	GenomicDataCenterId           string `json:"genomicDataCenterId"`
	ClinicalDataNodeId            string `json:"clinicalDataNodeId"`
	GenomicStudyType              string `json:"genomicStudyType"`
	GenomicStudySubtype           string `json:"genomicStudySubtype"`
	LabName                       string `json:"labName"`
	LabDataName                   string `json:"labDataName"`
	TissueTypeName                string `json:"tissueTypeName"`
	SequenceType                  string `json:"sequenceType"`
	SequenceSubType               string `json:"sequenceSubtype"`
	FragmentationMethod           string `json:"fragmentationMethod"`
	LibraryType                   string `json:"libraryType"`
	LibraryPrepKit                string `json:"libraryPrepKit"`
	LibraryPrepKitManufacturer    string `json:"libraryPrepKitManufacturer"`
	SequencerModel                string `json:"sequencerModel"`
	SequencerManufacturer         string `json:"sequencerManufacturer"`
	KitName                       string `json:"kitName"`
	KitManufacturer               string `json:"kitManufacturer"`
	EnrichmentKitManufacturer     string `json:"enrichmentKitManufacturer"`
	EnrichmentKitDescription      string `json:"enrichmentKitDescription"`
	SequencingLayout              string `json:"sequencingLayout"`
	TumorCellCountMethod          string `json:"tumorCellCountMethod"`
	BioinformaticsPipelineName    string `json:"bioinformaticsPipelineName"`
	BioinformaticsPipelineVersion string `json:"bioinformaticsPipelineVersion"`
	CallerUsedName                string `json:"callerUsedName"`
	CallerUsedVersion             string `json:"callerUsedVersion"`
}

//go:embed profiles.json
var profiles []byte

func ReadProfiles() []Klinik {
	var result []Klinik
	if err := json.Unmarshal(profiles, &result); err != nil {
		return []Klinik{}
	}
	return result
}

func FindKlinik(ik string) *Klinik {
	profiles := ReadProfiles()
	for _, klinik := range profiles {
		if klinik.Ik == ik {
			return &klinik
		}
	}
	return nil
}

func FindProfile(ik string, profileName string) *Profile {
	profiles := ReadProfiles()
	for _, klinik := range profiles {
		if klinik.Ik == ik {
			for _, profile := range klinik.Profiles {
				if profile.Name == profileName {
					return &profile
				}
			}
		}
	}
	return nil
}
