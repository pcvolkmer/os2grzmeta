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
	"syscall"

	"github.com/alecthomas/kong"
	"github.com/charmbracelet/huh"
	"github.com/go-sql-driver/mysql"
	"github.com/pcvolkmer/mv64e-grz-dto-go"
	"golang.org/x/term"
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
	SampleId string `help:"Einsendenummer" required:"NA"`
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
		fmt.Print("Passwort: ")
		if bytePw, err := term.ReadPassword(int(syscall.Stdin)); err == nil {
			cli.Password = string(bytePw)
		}
		println()
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

	data, err := fetchMetadata()
	if err != nil {
		log.Fatalf("Cannot fetch metadata: %s\n", err.Error())
	}
	j, err := json.MarshalIndent(data, "", "  ")
	if err := os.WriteFile(cli.Filename, j, 0644); err != nil {
		log.Fatal(err)
	}
}

func fetchMetadata() (*metadata.Metadata, error) {
	fallnummer := ""

	fallnummern, err := fetchFallnummern()

	if err == nil && len(fallnummern) > 1 {
		options := []huh.Option[string]{}
		for _, option := range fallnummern {
			options = append(options, huh.NewOption(option, option))
		}
		huh.NewSelect[string]().Title("Fallnummer").Options(options...).Value(&fallnummer)
	}

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
						LocalCaseID:    fallnummer,
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
							// Fixed value for UKW-CCC!
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
		JOIN dk_dnpm_therapieplan ON (dk_dnpm_therapieplan.ref_dnpm_klinikanamnese = dk_dnpm_kpa.id)
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

	if rows, err := db.Query(query, cli.SampleId, cli.SampleId, cli.SampleId); err == nil {
		var caseId sql.NullString
		for rows.Next() {
			if err := rows.Scan(&caseId); err == nil {
				result = append(result, caseId.String)
			}
		}
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
