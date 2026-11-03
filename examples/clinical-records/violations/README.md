# Seeded violations

Five broken versions of `surfaces.malloy`, each one defect a review of this package against
`policies/phi-access-and-minimum-necessary.md` must catch. They are stored as `.txt` so they never
compile with the clean package: Publisher compiles every `.malloy` file in a package, and three of
these refuse the load. To try one, copy the package, put the variant in place of
`surfaces.malloy`, and load the copy.

| Variant                        | Defect                                                                                       | Controls      | What Publisher does                                                                                                                                                                                                                                                                        |
| ------------------------------ | -------------------------------------------------------------------------------------------- | ------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `01-research-reidentifies`     | The de-identified research surface joins back to the full patient record                     | DI-03, AC-04  | Refuses the load: `research` carries no `phi.direct_identifier`, and the join exposes ten                                                                                                                                                                                                  |
| `02-mislabeled-identifier`     | The limited data set serves the encounter's record number, declared `#(classification) none` | LDS-01, CL-02 | **Loads.** The declaration is wrong, and the server cannot know it. Only a review that reads the values catches it                                                                                                                                                                         |
| `03-billing-all-organizations` | The billing surface keeps its role gate but drops its organization scope                     | MN-02         | **Loads.** Nothing mechanical is wrong: a role-gated surface over locked cores is the designed pattern, and the server cannot know MN-02 requires a scope. A check that encodes the policy's workforce table catches it: the data app's access grid flags the billing row, and MN-02 fails |
| `04-requester-asserted-scope`  | The clinicians' organization scope reads a given the requester sets                          | AC-03, MN-01  | Refuses the load: a published gate reads a given that is not `#(secure)`                                                                                                                                                                                                                   |
| `05-billing-reopened`          | The billing surface is re-opened to every caller with `#(authorize) true`                    | AC-01, AC-05  | Refuses the load: an open source author-joins the locked patient record                                                                                                                                                                                                                    |

The split is the point. Three of the five are mechanical, so the server refuses them before anything
is served. The other two are a model that is internally consistent and still wrong against the
policy: a declaration that is false (02), and a scope the policy requires that the model never
states (03). Catching them takes reading the policy and the data, which is the review skill's job.

Simply deleting a surface's gate is not among them, because it is not a leak: in this package every
surface extends a core that inherits its base's `#(authorize) false`, so a surface with no gate of
its own inherits the lock and refuses everyone. The pattern fails closed.
