# Seeded violations

Six broken versions of the package, each one defect a review of this package against
`policies/phi-access-and-minimum-necessary.md` must catch. They are stored as `.txt` so they never
compile with the clean package: Publisher compiles every `.malloy` file in a package, and three of
these refuse the load. To try one, copy the package, put the variant in place of the file it is
named for (`domains.malloy`, or `deidentify.malloy` for 06), and load the copy.

| Variant                        | Defect                                                                                                     | Controls      | What Publisher does                                                                                                                                                                                                                                                                       | `compliance.py check`                                          |
| ------------------------------ | ---------------------------------------------------------------------------------------------------------- | ------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------- |
| `01-research-reidentifies`     | The de-identified research domain joins back to the full patient record                                    | DI-03, AC-04  | Refuses the load: `research` carries no `phi.direct_identifier`, and the join exposes ten                                                                                                                                                                                                 | exit 1: fails AC-04, DI-03; 17 need review (not served)        |
| `02-mislabeled-identifier`     | The limited data set serves the encounter's record number, declared `#(classification) "none"`             | LDS-01, CL-02 | **Loads.** The declaration is wrong, and the server cannot know it. Only a review that reads the values catches it                                                                                                                                                                        | exit 1: fails CL-02, AC-04, MN-03, LDS-01                      |
| `03-billing-all-organizations` | The billing domain keeps its role gate but drops its organization scope                                    | MN-02         | **Loads.** Nothing mechanical is wrong: a role-gated domain over locked cores is the designed pattern, and the server cannot know MN-02 requires a scope. A check that encodes the policy's workforce table catches it: the data app's access grid flags the billing row, and MN-02 fails | exit 1: fails AC-01, MN-02                                     |
| `04-requester-asserted-scope`  | The clinicians' organization scope reads a given the requester sets                                        | AC-03, MN-01  | Refuses the load: a published gate reads a given that is not `#(secure)`                                                                                                                                                                                                                  | exit 1: fails AC-03, MN-01, MN-02; 16 need review (not served) |
| `05-billing-reopened`          | The billing domain is re-opened to every caller with `#(authorize) true`                                   | AC-01, AC-05  | Refuses the load: an open source author-joins the locked patient record                                                                                                                                                                                                                   | exit 1: fails AC-05, MN-02; 17 need review (not served)        |
| `06-birth-year-over-89`        | The de-identified patient publishes `birth_year` for every age, so a patient over 89 is identifiable by it | DI-01         | **Loads.** The year is a declared quasi-identifier and nothing mechanical is wrong. The research cap `patient.birth_year` min 1930 catches it: the value check reads down to 1909                                                                                                         | exit 1: fails CL-02, AC-04, MN-04, DI-01, DI-02, DI-03, DI-04  |

The split is the point. Three of the six are mechanical, so the server refuses them before anything
is served. The other three are a model that is internally consistent and still wrong against the
policy: a declaration that is false (02), a scope the policy requires that the model never
states (03), and a value the policy bars that the model publishes anyway (06). Catching them takes
reading the policy and the data, which is the review skill's job.

The last column is `compliance.py check` against a local Publisher: every variant exits 1 and the
clean package exits 0. A refused load fails the controls it breaks and leaves the rest as needing
review, because nothing is served to probe. A failing check on a domain fails every control that
cites that domain, which is why a variant lists more than its own defect.

Simply deleting a domain's gate is not among them, because it is not a leak: in this package every
domain extends a core that inherits its base's `#(authorize) false`, so a domain with no gate of
its own inherits the lock and refuses everyone. The pattern fails closed.
