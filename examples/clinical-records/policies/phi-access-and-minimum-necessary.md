# PHI Access and Minimum Necessary Policy

**Entity:** Riverbend Health (a fictional integrated delivery network; this policy governs the synthetic data in this package) · **Owner:** Privacy Officer · **Approver:** Compliance Committee · **Effective:** 2026-11-01 · **Next review:** 2027-11-01

Implements 45 CFR 164.502(b) and 164.514(d) (minimum necessary), 164.514(b) (de-identification, Safe Harbor), 164.514(e) (limited data sets), 164.308(a)(4) (information access management), 164.312(a)(1) (access control) and 164.312(b) (audit controls). Crosswalk: SOC 2 CC6.1, CC6.3, CC7.2; NIST 800-53 AC-3, AC-6, AU-2, RA-2.

## Purpose and scope

Riverbend's analytics platform serves patient data to people and to AI agents acting for people. This policy states who may see which categories of protected health information through that platform, in what form, and how the platform must prove it. It applies to every analytical surface the platform publishes: a surface is any named entry point a person or agent may query. It does not govern the clinical systems of record, which have their own access policy, nor the warehouse's own administrative access, which the Information Security Policy governs.

A control in this policy is satisfied only by a mechanism the platform enforces. A note in a wiki, a team's custom, or a prompt given to an agent is not a control.

## Definitions

**Protected health information (PHI)** is individually identifiable health information held or transmitted by Riverbend, in any form.

**Direct identifiers** are the identifier categories listed in Appendix A (the Safe Harbor list, 164.514(b)(2)(i)): names, geographic subdivisions smaller than a state, dates other than year, telephone and fax numbers, email addresses, Social Security numbers, medical record numbers, health plan and account numbers, certificate and license numbers, vehicle and device identifiers, URLs and IP addresses, biometric identifiers, full-face images, and any other unique identifying number, characteristic, or code.

**Quasi-identifiers** are elements that identify a person in combination though not alone: dates to the day or month, ages over 89, geography below the state level, and small-population three-digit ZIP codes (Appendix A, items 2 and 3).

**Clinical data** is diagnoses, procedures, medications, immunizations, observations, allergies, care plans, imaging and devices, and the encounters that carry them.

**Financial data** is claim amounts, charges, coverage, payer and plan information, and a patient's healthcare expenses.

**Full PHI** is data in which direct identifiers are present.

**A limited data set** (164.514(e)) is PHI from which every direct identifier has been removed except dates, city, state, ZIP code and age, and which may be used only for research, public health or health care operations under a data use agreement.

**De-identified data** (164.514(b)) is data from which every Safe Harbor identifier has been removed, with dates reduced to the year, ages over 89 reported as 90 or over, and geography reduced to the state or to a three-digit ZIP code of a population above 20,000, and about which Riverbend has no actual knowledge that it could identify a person. De-identified data is not PHI.

## Data classification

Every field the platform publishes carries exactly one of the following classes, or more than one where a field falls under several regimes. The class is a statement about the field, not about who may read it; the workforce classes below say who.

| Class | Meaning | Examples |
| --- | --- | --- |
| `phi.direct_identifier` | A Safe Harbor identifier (Appendix A) | SSN, name, street address, birth date, medical record number, license number |
| `phi.quasi_identifier` | Identifying in combination | city, county, ZIP, age, dates to the day, provider and organization identity |
| `phi.clinical` | Clinical content about an individual | condition, procedure, medication, observation value, reason for visit |
| `phi.financial` | Financial content about an individual | claim cost, coverage, payer, healthcare expenses |
| `none` | Nothing this policy governs | a code system name, a count of rows, a year |

A field derived from a classified field carries the class of what it is derived from unless a de-identification control (DI-) says otherwise. A count of patients carries `none`; a sum of a patient's expenses carries `phi.financial`; a birth year derived from a birth date carries `phi.quasi_identifier`, not `phi.direct_identifier`, because DI-01 says a year alone is not an identifier.

**CL-01** Every field on every published surface carries a classification. An unclassified field is a field this policy has nothing to say about, which is not permitted on a surface that serves PHI.

**CL-02** Classification is declared where a field is defined and travels with it. A surface may not reclassify a field it did not define except by a de-identification control.

## Workforce classes (164.514(d)(2))

Riverbend identifies the following classes of persons who need access to PHI through the platform, and the category of PHI each needs. Access is granted to a class, never to an individual, and an agent acting for a person holds that person's class and no more.

| Class | Needs access to | Does not need |
| --- | --- | --- |
| Treating clinician | Full PHI, all classes, for patients with an encounter at an organization the clinician belongs to | Patients of other organizations |
| Care team (nursing, scheduling) | The same scope as a treating clinician | Financial detail beyond coverage status |
| Billing and claims | Direct identifiers needed to bill, encounter and procedure codes, and all financial data, for the organizations they bill for | Observation values, care plans, allergies, imaging |
| Quality and population health analyst | A limited data set: clinical and financial data with dates, city, state, ZIP and age, no direct identifiers | Any direct identifier |
| IRB-approved researcher | De-identified data only | Any identifier, any date below the year, any geography below the state |
| External data partner | A limited data set, under an executed data use agreement | Anything outside the agreement's scope; anything without an agreement |
| Privacy officer and auditor | The platform's access records and governance exports | Patient data through this platform |

## Access controls

**AC-01** Every published surface that serves PHI is gated by workforce class. A request that does not carry a verified workforce class is refused, not served an empty result. The class is asserted by Riverbend's identity provider, never by the requester.

**AC-02** The base tables the platform models are not queryable by anyone. Every query enters through a published surface.

**AC-03** A surface's gate reads only attributes the identity provider sets. A gate that reads an attribute the requester can supply is not a control and does not satisfy AC-01.

**AC-04** A surface serves only the classes of data its workforce class needs, and declares the classes it serves. A surface that serves a class outside its declaration is a violation, however the class got there: by a field, a derivation, or a join.

**AC-05** A join from a published surface to a restricted table does not extend the surface's access. The surface's own gate must scope the joined rows, or the join must be keyed on the requester's scope.

## Minimum necessary

**MN-01** A treating clinician, and the care team, may read a patient's full PHI only when the patient has an encounter at an organization the clinician belongs to. Organization membership is an attribute of identity, not a parameter of the request.

**MN-02** Billing and claims may read a patient's direct identifiers, encounter and procedure codes, and financial data, for the organizations they bill for, and may not read observation values, care plans, allergies, imaging or device data.

**MN-03** A quality and population health analyst reads a limited data set and no direct identifier, through a surface that cannot produce one.

**MN-04** A researcher reads de-identified data and nothing else, through a surface that cannot produce an identifier, a date below the year, or geography below the state.

**MN-05** An external data partner reads a limited data set only, only under an executed data use agreement, and only for the term of the agreement. The agreement is an attribute of identity.

## De-identification and limited data sets

**DI-01** A de-identified surface reduces every date directly related to an individual to its year, reports every age over 89 as "90 or over", and carries no element below the year. A year alone is a quasi-identifier, not a direct identifier.

**DI-02** A de-identified surface reduces geography to the state, or to the first three digits of a ZIP code where the population of that three-digit area exceeds 20,000 (Appendix B lists the areas that do not qualify, which are reported as `000`).

**DI-03** A de-identified surface carries no field classified `phi.direct_identifier`, and no field derived from one unless a DI- control has reduced it. Record identifiers used to join tables are not served.

**DI-04** A de-identified surface carries no free-text field, because free text can carry any identifier.

**LDS-01** A limited data set surface carries no field classified `phi.direct_identifier`. It may carry dates, city, state, ZIP and age.

**LDS-02** A limited data set surface is gated on an executed data use agreement for an external partner, and on workforce class for an internal analyst.

## Audit and evidence

**AU-01** Every query against a published surface produces a record of who asked (by identity provider assertion), which surface, which fields and classes were read, the gate decision, and the result's size. Records are retained for six years (164.316(b)(2)).

**AU-02** The platform produces, on demand and from the model it is serving rather than from documents about it, an inventory of every published surface, its gate, the classes it declares, and every field it serves with its classification and derivation. This inventory is the evidence of CL-01, AC-02, AC-04 and the MN- controls, and is retained with each release of the model.

**AU-03** A change to the model is reviewed against this policy before it is served. A change that would violate a control is refused, not deployed with a warning.

## Appendix A: Safe Harbor identifiers (164.514(b)(2)(i))

1. Names
2. All geographic subdivisions smaller than a state, including street address, city, county, precinct, ZIP code and their equivalent geocodes, except the initial three digits of a ZIP code if the three-digit area contains more than 20,000 people
3. All elements of dates (except year) directly related to an individual, including birth date, admission date, discharge date and date of death, and all ages over 89
4. Telephone numbers
5. Fax numbers
6. Email addresses
7. Social Security numbers
8. Medical record numbers
9. Health plan beneficiary numbers
10. Account numbers
11. Certificate or license numbers
12. Vehicle identifiers and serial numbers, including license plate numbers
13. Device identifiers and serial numbers
14. Web URLs
15. IP addresses
16. Biometric identifiers, including finger and voice prints
17. Full-face photographs and comparable images
18. Any other unique identifying number, characteristic or code

## Appendix B: three-digit ZIP areas below 20,000 population

The seventeen three-digit ZIP code areas whose population is 20,000 or fewer per the current Census, which a de-identified surface reports as `000`, are listed in `data/zip3_small_population.parquet` in this package. The list is maintained by the Privacy Officer and updated when the Census publishes new figures.

## Approval

Approved by the Compliance Committee of Riverbend Health on 2026-11-01.
