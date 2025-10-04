/*
 * This file is part of os2grzmeta
 *
 * Copyright (C) 2025 the original author or authors.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strconv"

	"github.com/alecthomas/kong"
	"github.com/charmbracelet/huh"
	"github.com/go-sql-driver/mysql"
	"github.com/pcvolkmer/mv64e-grz-dto-go"
)

var (
	cli     *CLI
	context *kong.Context
	db      *sql.DB
)

type Globals struct {
	User     string `short:"U" help:"Database username" required:"NA"`
	Password string `short:"P" help:"Database password"`
	Host     string `short:"H" help:"Database host" default:"localhost"`
	Port     int    `help:"Database port" default:"3306"`
	Ssl      string `help:"SSL-Verbindung ('true', 'false', 'skip-verify', 'preferred')" default:"false"`
	Database string `short:"D" help:"Database name" default:"onkostar"`
	SampleId string `help:"Einsendenummer"`
	Filename string `help:"Ausgabedatei" required:"NA"`
}

type CLI struct {
	Globals
}

func initCLI() {
	cli = &CLI{
		Globals: Globals{},
	}
	homedir, _ := os.UserHomeDir()
	context = kong.Parse(cli,
		kong.Name("os2grzmeta"),
		kong.Description("A simple tool to export GRZ metadata template from Onkostar database"),
		kong.UsageOnError(),
		kong.ConfigureHelp(kong.HelpOptions{
			Compact: true,
		}),
		kong.Configuration(kong.JSON, fmt.Sprintf("%s/.osdb-config.json", homedir)),
	)
}

func initDb(dbCfg mysql.Config) (*sql.DB, error) {
	if dbx, err := sql.Open("mysql", dbCfg.FormatDSN()); err == nil {
		if err := dbx.Ping(); err == nil {
			return dbx, nil
		} else {
			return nil, err
		}
	} else {
		return nil, err
	}
}

func main() {
	initCLI()

	if len(cli.Password) == 0 {
		_ = huh.NewInput().Title("Passwort").
			Value(&cli.Password).
			EchoMode(huh.EchoModePassword).
			WithTheme(huh.ThemeBase16()).
			Run()
	}

	dbCfg := mysql.Config{
		User:                 cli.User,
		Passwd:               cli.Password,
		Net:                  "tcp",
		Addr:                 fmt.Sprintf("%s:%d", cli.Host, cli.Port),
		DBName:               cli.Database,
		AllowNativePasswords: true,
		TLSConfig:            cli.Ssl,
	}

	if dbx, dbErr := initDb(dbCfg); dbErr == nil {
		db = dbx
		defer func(db *sql.DB) {
			err := db.Close()
			if err != nil {
				log.Println("Cannot close database connection")
			}
		}(db)
	} else {
		log.Fatalf("Cannot connect to Database: %s\n", dbErr.Error())
	}

	form := NewForm()
	form.Init()
	_ = form.Run()

	data, _ := fetchMetadata(form.selectedFallnummer)
	if data == nil {
		log.Fatalf("Cannot fetch metadata")
	}

	data.Submission.LocalCaseID = form.selectedFallnummer
	data.Submission.ClinicalDataNodeID = form.selectedKdk
	data.Submission.GenomicDataCenterID = form.selectedGrz

	if profile := FindProfile(form.selectedIk, form.selectedProfile); profile != nil {
		data.Submission.GenomicStudyType = metadata.GenomicStudyType(profile.GenomicStudyType)
		data.Submission.GenomicStudySubtype = metadata.GenomicStudySubtype(profile.GenomicStudySubtype)
		data.Submission.LabName = profile.LabName
		data.Donors[0].LabData[0].LabDataName = profile.LabDataName
		data.Donors[0].LabData[0].TissueTypeName = profile.TissueTypeName
		data.Donors[0].LabData[0].SequenceType = metadata.SequenceType(profile.SequenceType)
		data.Donors[0].LabData[0].SequenceSubtype = metadata.SequenceSubtype(profile.SequenceSubType)
		data.Donors[0].LabData[0].FragmentationMethod = metadata.FragmentationMethod(profile.FragmentationMethod)
		data.Donors[0].LabData[0].LibraryType = metadata.LibraryType(profile.LibraryType)
		data.Donors[0].LabData[0].LibraryPrepKit = profile.LibraryPrepKit
		data.Donors[0].LabData[0].LibraryPrepKitManufacturer = profile.LibraryPrepKitManufacturer
		data.Donors[0].LabData[0].SequencerModel = profile.SequencerModel
		data.Donors[0].LabData[0].SequencerManufacturer = profile.SequencerManufacturer
		data.Donors[0].LabData[0].KitName = profile.KitName
		data.Donors[0].LabData[0].KitManufacturer = profile.KitManufacturer
		data.Donors[0].LabData[0].EnrichmentKitManufacturer = metadata.EnrichmentKitManufacturer(profile.EnrichmentKitManufacturer)
		data.Donors[0].LabData[0].EnrichmentKitDescription = profile.EnrichmentKitDescription
		data.Donors[0].LabData[0].SequencingLayout = metadata.SequencingLayout(profile.SequencingLayout)
		data.Donors[0].LabData[0].TumorCellCount[0].Method = metadata.Method(profile.TumorCellCountMethod)
		data.Donors[0].LabData[0].SequenceData.BioinformaticsPipelineName = profile.BioinformaticsPipelineName
		data.Donors[0].LabData[0].SequenceData.BioinformaticsPipelineVersion = profile.BioinformaticsPipelineVersion
		data.Donors[0].LabData[0].SequenceData.CallerUsed = append(data.Donors[0].LabData[0].SequenceData.CallerUsed, metadata.CallerUsed{
			Name:    profile.CallerUsedName,
			Version: profile.CallerUsedVersion,
		})
	}

	j, _ := json.MarshalIndent(data, "", "  ")
	if err := os.WriteFile(cli.Filename, j, 0644); err != nil {
		log.Fatal(err)
	} else {
		fmt.Printf("\033[32m✅ Ermittelte Daten wurden als Vorlage in die Datei '%s' geschrieben.\033[0m\n", cli.Filename)
	}
}

type Form struct {
	innerForm            *huh.Form
	availableFallnummern []string
	selectedIk           string
	selectedProfile      string
	selectedKdk          string
	selectedGrz          string
	selectedFallnummer   string
}

func NewForm() *Form {
	return &Form{
		availableFallnummern: make([]string, 0),
	}
}

func (f *Form) Run() error {
	return f.innerForm.Run()
}

func (f *Form) Init() {
	ikOptions := []huh.Option[string]{}
	for _, option := range ReadProfiles() {
		ikOptions = append(ikOptions, huh.NewOption(fmt.Sprintf("%s - %s", option.Ik, option.Name), option.Ik))
	}

	f.innerForm = huh.NewForm(
		huh.NewGroup(
			huh.NewInput().
				Title("Einsendenummer").
				Value(&cli.SampleId),
			huh.NewSelect[string]().
				Title("Fallnummer").
				OptionsFunc(func() []huh.Option[string] {
					fallnummerOptions := []huh.Option[string]{
						huh.NewOption("--- (Keine Angabe)", ""),
					}
					if fallnummern, err := fetchFallnummern(); err == nil {
						for _, option := range fallnummern {
							fallnummerOptions = append(fallnummerOptions, huh.NewOption(option, option))
						}
					}
					return fallnummerOptions
				}, &cli.SampleId).
				Key("Fallnummer").
				Description("Fallnummer für das Modellvorhaben aus Formular 'DNPM Klinik/Anamnese'"),
			huh.NewSelect[string]().
				Title("Leistungserbringer").
				Options(ikOptions...).
				Value(&f.selectedIk),
			huh.NewSelect[string]().
				Title("LabData-Profil").
				OptionsFunc(func() []huh.Option[string] {
					options := []huh.Option[string]{}
					for _, klinik := range ReadProfiles() {
						if klinik.Ik == f.selectedIk || len(f.selectedIk) == 0 {
							options = append(options, huh.NewOption("--- (Kein Profil anwenden)", ""))
							for _, profile := range klinik.Profiles {
								options = append(options, huh.NewOption(profile.Name, profile.Name))
							}
						}
					}
					return options
				}, &f.selectedIk).
				Value(&f.selectedProfile).
				DescriptionFunc(func() string {
					if klinik := FindKlinik(f.selectedIk); klinik != nil {
						return fmt.Sprintf("Auswahl für '%s' - Siehe auch Formular 'Molekulargenetische Untersuchung'", klinik.Name)
					} else {
						return "Siehe auch Formular 'Molekulargenetische Untersuchung'"
					}
				}, &f.selectedIk).
				Description("Profil for LabData - Siehe auch Formular 'Molekulargenetische Untersuchung'"),
			huh.NewSelect[string]().
				Title("Genomrechenzentrum").
				Options(
					huh.NewOption("GRZK00001 (GRZ Köln)", "GRZK00001"),
					huh.NewOption("GRZTUE002 (GRZ Tübingen)", "GRZTUE002"),
					huh.NewOption("GRZHD0003 (GRZ Heidelberg)", "GRZHD0003"),
					huh.NewOption("GRZDD0004 (GRZ Dresden)", "GRZDD0004"),
					huh.NewOption("GRZM00006 (GRZ München)", "GRZM00006"),
					huh.NewOption("GRZB00007 (GRZ Berlin)", "GRZB00007"),
				).
				Value(&f.selectedGrz).
				Description("Zu verwendendes Genomrechenzentrum"),
			huh.NewSelect[string]().
				Title("Klinischer Datenknoten").
				Options(
					huh.NewOption("KDKDD0001 - GfH-NET (Universitätsklinikum Dresden)", "KDKDD0001"),
					huh.NewOption("KDKTUE002 - NSE (Universitätsklinikum Tübingen)", "KDKTUE002"),
					huh.NewOption("KDKL00003 - DK-FBREK (Universität Leipzig)", "KDKL00003"),
					huh.NewOption("KDKL00004 - DK-FDK (Universität Leipzig)", "KDKL00004"),
					huh.NewOption("KDKTUE005 - DNPM (Universitätsklinikum Tübingen)", "KDKTUE005"),
					huh.NewOption("KDKHD0006 - NCT/DKTK MASTER (NCT Heidelberg)", "KDKHD0006"),
					huh.NewOption("KDKK00007 - nNGM (Universitätsklinikum Köln)", "KDKK00007"),
				).
				Value(&f.selectedKdk).
				Description("Zu verwendender klinischer Datenknoten"),
		).Title("Weitere Angaben zum Fall, Genomrechenzentrum und zum klinischen Datenknoten"),
	).
		WithTheme(huh.ThemeBase16())
}

func fetchMetadata(fallnummer string) (*metadata.Metadata, error) {
	query := `SELECT
				organisationunit.identifier AS submission_labname,
				CASE
					WHEN kostentraegertyp = 'GKV' THEN 'GKV'
					WHEN kostentraegertyp = 'PKV' THEN 'PKV'
					ELSE 'UNK'
					END AS submission_coveragetype,
    			patient.patienten_id as donors_items_pseudonym,
				CASE
					WHEN patient.geschlecht = 'm' THEN 'male'
					WHEN patient.geschlecht = 'w' THEN 'female'
					WHEN patient.geschlecht = 'u' THEN 'unknown'
					ELSE 'other'
					END AS donors_items_gender,
				CONCAT(prop_probenmaterial.shortdesc, ' ', prop_nukleinsaeure.shortdesc) AS donors_items_labdata_items_labdataname,
				dk_molekulargenetik.entnahmedatum AS donors_items_labdata_items_sampledate,
				CASE
				    WHEN dk_molekulargenetik.materialfixierung = 2 THEN 'cryo-frozen'
				    WHEN dk_molekulargenetik.materialfixierung = 3 THEN 'ffpe'
				    WHEN dk_molekulargenetik.materialfixierung = 9 THEN 'unknown'
				    ELSE 'other'
				    END AS donors_items_labdata_items_sampleconservation,
				LOWER(prop_nukleinsaeure.shortdesc) AS donors_items_labdata_items_sequencetype,
				CASE
					WHEN dk_molekulargenetik.artdersequenzierung = 'WES' THEN 'wes'
					WHEN dk_molekulargenetik.artdersequenzierung = 'WGS' THEN 'wgs'
					WHEN dk_molekulargenetik.artdersequenzierung = 'PanelKit' THEN 'panel'
					WHEN dk_molekulargenetik.artdersequenzierung = 'X' THEN 'unknown'
					ELSE 'other'
					END AS donors_items_labdata_items_librarytype,
				dk_molekulargenetik.tumorzellgehalt AS donors_items_labdata_items_tumorCellCount_items_count,
				CASE
					WHEN dk_molekulargenetik.referenzgenom = 'HG19' THEN 'GRCh37'
					WHEN dk_molekulargenetik.referenzgenom = 'HG38' THEN 'GRCh38'
					END AS donors_items_labdata_items_sequencedata_referencegenome,
				dk_molekulargenetik.panel AS x_panel # Use this to select default Kit info
			FROM dk_molekulargenetik
			JOIN prozedur ON (prozedur.id = dk_molekulargenetik.id)
			JOIN patient ON (patient.id = prozedur.patient_id)
			LEFT JOIN organisationunit ON (organisationunit.id = dk_molekulargenetik.durchfuehrendeoe_fachabteilung)
			LEFT JOIN property_catalogue_version_entry AS prop_nukleinsaeure ON (
				prop_nukleinsaeure.property_version_id = dk_molekulargenetik.nukleinsaeure_propcat_version
					AND prop_nukleinsaeure.code = dk_molekulargenetik.nukleinsaeure)
			LEFT JOIN property_catalogue_version_entry AS prop_probenmaterial ON (
				prop_probenmaterial.property_version_id = dk_molekulargenetik.probenmaterial_propcat_version
					AND prop_probenmaterial.code = dk_molekulargenetik.probenmaterial)
			
			# Hier die Einsendenummer aus Rohdaten-Datei in diesem Format einfügen
			WHERE einsendenummer = ?`

	var result = metadata.Metadata{}

	if rows, err := db.Query(query, cli.SampleId); err == nil {
		var submissionLabname sql.NullString
		var submissionCoveragetype sql.NullString
		var donorsPseudonym sql.NullString
		var donorsGender sql.NullString
		var donorsLabdataLabdataname sql.NullString
		var donorsLabdataSampledate sql.NullString
		var donorsLabdataSampleconservation sql.NullString
		var donorsLabdataSequencetype sql.NullString
		var donorsLabdataLibrarytype sql.NullString
		var donorsLabdataTumorcellcount sql.NullString
		var donorsLabdataSequencedataReferencegenome sql.NullString
		var xPanel sql.NullString
		for rows.Next() {
			if err := rows.Scan(
				&submissionLabname,
				&submissionCoveragetype,
				&donorsPseudonym,
				&donorsGender,
				&donorsLabdataLabdataname,
				&donorsLabdataSampledate,
				&donorsLabdataSampleconservation,
				&donorsLabdataSequencetype,
				&donorsLabdataLibrarytype,
				&donorsLabdataTumorcellcount,
				&donorsLabdataSequencedataReferencegenome,
				&xPanel,
			); err == nil {
				if len(result.Donors) == 0 {
					result.Submission = metadata.Submission{
						SubmissionType: metadata.Initial,
						CoverageType:   metadata.CoverageType(submissionCoveragetype.String),
						DiseaseType:    metadata.Oncological,
					}

					result.Donors = []metadata.Donor{
						{
							DonorPseudonym: donorsPseudonym.String,
							Gender:         metadata.Gender(donorsGender.String),
							LabData:        []metadata.LabDatum{},
							// Onkostar only holds index patient data
							Relation: metadata.Index,
						},
					}

					if consentMv, err := fetchMvConsent(fallnummer); err == nil && consentMv != nil {
						result.Donors[0].MvConsent = *consentMv
					}
				}

				tumorCellCount, _ := strconv.ParseFloat(donorsLabdataTumorcellcount.String, 64)

				labData := metadata.LabDatum{
					Barcode:            "NA",
					LabDataName:        donorsLabdataLabdataname.String,
					SampleDate:         donorsLabdataSampledate.String,
					SampleConservation: metadata.SampleConservation(donorsLabdataSampleconservation.String),
					SequenceType:       metadata.SequenceType(donorsLabdataSequencetype.String),
					LibraryType:        metadata.LibraryType(donorsLabdataLibrarytype.String),
					TumorCellCount: []metadata.TumorCellCount{
						{
							Count: tumorCellCount,
							// Fixed value for UKW-CCC! Will be overwritten by profile
							Method: metadata.Pathology,
						},
					},
					SequenceData: &metadata.SequenceData{
						// As mapped from Onkostar HG19 => GRCh37!
						ReferenceGenome: metadata.ReferenceGenome(donorsLabdataSequencedataReferencegenome.String),
						Files:           []metadata.File{},
					},
				}

				result.Donors[0].LabData = append(result.Donors[0].LabData, labData)
			} else {
				return nil, err
			}
		}
	} else {
		return nil, err
	}

	return &result, nil
}

func fetchFallnummern() ([]string, error) {
	query := `SELECT DISTINCT
			dk_dnpm_kpa.fallnummermv
		FROM dk_dnpm_kpa
		JOIN dk_dnpm_therapieplan ON (dk_dnpm_therapieplan.ref_dnpm_klinikanamnese = dk_dnpm_kpa.id AND dk_dnpm_kpa.fallnummermv = ?)
		OR dk_dnpm_therapieplan.id IN (
			SELECT hauptprozedur_id FROM dk_molekulargenetik
			JOIN dk_dnpm_uf_rebiopsie ON (dk_dnpm_uf_rebiopsie.ref_molekulargenetik = dk_molekulargenetik.id)
			JOIN prozedur ON (prozedur.id = dk_dnpm_uf_rebiopsie.id)
			WHERE einsendenummer = ?
		)
		OR dk_dnpm_therapieplan.id IN (
			SELECT hauptprozedur_id FROM dk_molekulargenetik
			JOIN dk_dnpm_uf_reevaluation ON (dk_dnpm_uf_reevaluation.ref_molekulargenetik = dk_molekulargenetik.id)
			JOIN prozedur ON (prozedur.id = dk_dnpm_uf_reevaluation.id)
			WHERE einsendenummer = ?
		)
		OR dk_dnpm_therapieplan.id IN (
			SELECT hauptprozedur_id FROM dk_molekulargenetik
			JOIN dk_dnpm_uf_einzelempfehlung ON (dk_dnpm_uf_einzelempfehlung.ref_molekulargenetik = dk_molekulargenetik.id)
			JOIN prozedur ON (prozedur.id = dk_dnpm_uf_einzelempfehlung.id)
			WHERE einsendenummer = ?
		)`

	var result []string

	if rows, err := db.Query(query, cli.SampleId, cli.SampleId, cli.SampleId, cli.SampleId); err == nil {
		var caseId sql.NullString
		for rows.Next() {
			if err := rows.Scan(&caseId); err == nil {
				result = append(result, caseId.String)
			}
		}
	} else {
		return nil, err
	}

	return result, nil
}

func fetchMvConsent(caseId string) (*metadata.MvConsent, error) {
	query := `SELECT
			date,
			version,
			sequencing,
			caseidentification,
			reidentification
		FROM dk_dnpm_uf_consentmvverlauf
		JOIN prozedur ON (prozedur.id = dk_dnpm_uf_consentmvverlauf.id)
		WHERE prozedur.hauptprozedur_id IN (
			SELECT dk_dnpm_consentmv.id
			FROM dk_dnpm_kpa
			JOIN dk_dnpm_consentmv ON (dk_dnpm_consentmv.id = dk_dnpm_kpa.consentmv64e)
			WHERE fallnummermv = ?
		)
		ORDER BY dk_dnpm_uf_consentmvverlauf.date DESC
		LIMIT 1;`

	if rows, err := db.Query(query, caseId); err == nil {
		var date sql.NullString
		var version sql.NullString
		var sequencing sql.NullString
		var caseidentification sql.NullString
		var reidentification sql.NullString

		for rows.Next() {
			if err := rows.Scan(&date, &version, &sequencing, &caseidentification, &reidentification); err == nil {
				mvConsent := metadata.MvConsent{
					PresentationDate: &date.String,
					Version:          version.String,
					Scope: []metadata.Scope{
						{
							Type:   metadata.Type(sequencing.String),
							Date:   date.String,
							Domain: metadata.MvSequencing,
						},
						{
							Type:   metadata.Type(reidentification.String),
							Date:   date.String,
							Domain: metadata.ReIdentification,
						},
						{
							Type:   metadata.Type(caseidentification.String),
							Date:   date.String,
							Domain: metadata.CaseIdentification,
						},
					},
				}

				return &mvConsent, nil
			}
		}
	}

	return nil, nil
}
