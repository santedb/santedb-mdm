# SanteDB Master Data Management 

This readme provides technical documentation on the MDM plugin for SanteDB. Overall architecture documentation can be found
on the [SanteDB WIKI](https://help.santesuite.org/santedb/architecture/data-storage-patterns/master-data-storage).

The use case for the Master Data Management plugin is to allow robust linking of records from multiple source systems
to a single MASTER (aka golden, enterprise, etc.) record. The goals of the MDM record plugin are:

* Allow source systems complete autonomy to edit/change their own source data feeds without impacting data feeds from other systems
* Ensure that all data from sensitive data sources (like HIV clinics) are not disclosed within the golden record
* Provide a transparent interface for callers of FHIR or HL7v2 to feed data to the MDM layer without knowing the specifics (i.e. it is transparent to the caller)
* Provide system administrators which a clear understanding of where data within their CDR is sourced
* Provide a mechanism for generated data (like identifiers, and tokens, etc.) to be segregated from source systems.

## Relationship Type Registrations

The MDM layer registers the following relationship types.

Mnemonic|Use
-|-
MDM-Master|Links a SOURCE/LOCAL entity to a MASTER entity
MDM-OriginalMaster|Whenever the MDM layer changes the MASTER automatically
MDM-RecordOfTruth|Links a MDM MASTER to a record which is promoted to RECORD OF TRUTH (this record trumps all source system)
MDM-Duplicate|Identifies a SOURCE was detected as a duplicate or a candidate of a MASTER
MDM-IgnoreCandidateLocalRecord|Indicates that the SOURCE and MASTER have been flagged as NOT the same and any future attempt to flag the record should be ignored.

**Note:** The MDM layer provides its own identiifer based `IRecordMatchingService` which wraps the configured `IRecordMatchingService` whcih allows the MDM layer 
to match records on identifiers, even when no other matching solution is configured.

## Behaviors

### Case 1: Register New , Distinct Source Record

This test case ensures that the basic test case is fulfilled in that a new record is created and a master is established for that record.

Pre-Conditions:
* None

Test Steps:
* `SOURCE_A` with minimal demographic data is registered (external id: `MDM-01`)

Outcomes: 
* MDM layer establishes `MASTER_A` 
* MDM layer links `SOURCE_A` with `MASTER_A` with relationship `MDM-Master` and classification `AUTO`
* Querying for `MDM-01` results in `MASTER_A` being returned
* `MASTER_A` contains the properties from `SOURCE_A`

### Case 2: Register New , Duplicate Source Record

This test case ensures that when a new record matches an existing record (as defined in the matcher) the new source record is linked. This matching can be 
on identifier (like an foreign identifier).

Pre-Conditions:
* `SOURCE_A` with minimal demographic data is registered (external id: `MDM-02A`)
* `MASTER_A` has been established for `SOURCE_A`

Test Steps:
* `SOURCE_B` with matching demographics to `SOURCE_A` is registered  (external id: `MDM-02B`)

Outcomes:
* MDM layer links `SOURCE_B` with `MASTER_A` with relationship `MDM-Master` and classification `AUTO`
* Querying for `MDM-02A` or `MDM-02B` results in `MASTER_A` being returned
* `MASTER_A` contains both identifiers `MDM-02A` and `MDM-02B`
* `MASTER_A` contains only one name (as they are identical)

### Case 3: Register New , Suspected Duplicate Record

This test case ensures that when a new source record is registered which has sufficient matching properties to be a `Probable` classification, that the
MDM layer establishes appropriate linkages.

Pre-Conditions:
* `SOURCE_A` with minimal demographic data is registered (external id: `MDM-03A`)
* `MASTER_A` has been established for `SOURCE_A`

Test Steps:
* `SOURCE_B` with demographics which match `SOURCE_A` (with the exception of `MutlipleBirthIndicator` being different) is registered (external id: `MDM-03B`)

Outcomes:
* MDM layer establishes `MASTER_B` for record `SOURCE_B` 
* `SOURCE_B` is linked to `MASTER_B` with relationship type `MDM-Master` and class `AUTO`
* `SOURCE_B` is linked to `MASTER_A` with relationship type `MDM-Duplicate` and class `AUTO`

### Case 4: Updating a Source Record Maintains Master Relationship

This test case ensures that a master record with only one source record maintains its link with its source even when the source is updated to 
wildly different inforation.

Pre-Conditions:
* `SOURCE_A` with minimal demographic data is registered (external id: `MDM-04`)
* `MASTER_A` has been established for `SOURCE_A`

Test Steps:
* Chagne all the properties of `SOURCE_A` to new values
* Save `SOURCE_A` 

Outcomes:
* `MASTER_A` remains established, no new master is created
* `SOURCE_A` remains pointed as `MASTER_A`
* All synthesized data in `MASTER_A` matches the updates to `SOURCE_A`

### Case 5: Update a Candidate Record to match the Master

This test case ensures that a source record is flagged as a candidate record. At a later time, when an update to the candidate is received and the candidate 
now matches the master it was a candidate for, the relationship is changed such that the candidate is now a linked master.

Pre-Conditions:
* `SOURCE_A` with minimal demographic data is registered (external id: `MDM-05A`)
* `MASTER_A` has been established for `SOURCE_A`
* `SOURCE_B` with demographic data similar to `SOURCE_A` is registered (external id: `MDM-05B`)
* `MASTER_B` has been established for `SOURCE_B`
* `SOURCE_B` has a relationship with `MASTER_A` of type `MDM-Duplicate` and class `AUTO`

Test Steps:
* Update the properties in `SOURCE_B` so that they match `SOURCE_A` (keep the identifiers the same)
* Save `SOURCE_B`

Outcomes:
* `SOURCE_B` points to `MASTER_A` with relationship `MDM-Master` and class `AUTO`
* `SOURCE_A` remains pointed to `MASTER_A` with relationship `MDM-Master` and class `AUTO`
* `SOURCE_B` no longer has a `MDM-Duplicate` relationship with `MASTER_A`
* `SOURCE_B` is linked to `MASTER_B` with relationship `MDM-OriginalMaster` and class `AUTO`
* `MASTER_B` is obsoleted and does not appear in queries
* `MASTER_A` has a relationship with `MASTER_B` of type `REPLACES`
* Searching for `MDM-04A` or `MDM-04B` returns `MASTER_A`

### Case 6: Update a Linked Source Such it Doesn't Match the Master

In this test case, a master record contains two sources which had previously been automatically established. The source system updates one of the source records
such that the matching engine no longer deems the source a match with the master. The source which was updated should be detached from the master.

Pre-Conditions:
* `SOURCE_A` is registered with minimal demographics (external id: `MDM-06A`)
* `SOURCE_A`  points to `MASTER_A` with relationship `MDM-Master` and class `AUTO`
* `SOURCE_B` is registered with same demographics as `SOURCE_A` (external id: `MDM-06B`)
* `SOURCE_B` points to `MASTER_A` with relationship `MDM-Master` and class `AUTO`

Test Steps:
* `SOURCE_B` properties are changed to vary wildly from `SOURCE_A`
* `SOURCE_B` is saved 

Outcomes:
* `SOURCE_A` remains pointed at `MASTER_A` with type `MDM-Master` and class `AUTO`
* `SOURCE_B` points as `MASTER_A` with type `MDM-OriginalMaster` and class `AUTO`
* `MASTER_B` is established in the database
* `SOURCE_B` points to `MASTER_B` with type `MDM-Master` and class `AUTO`
* Searching for `MDM-05A` returns `MASTER_A`
* Searching for `MDM-05B` returns `MASTER_B`

### Case 7: Manual Reconciliation of Candidate Record

This test case ensures that when a candidate record is manually reconciled to be a match, that appropriate relationship steps occur.

Pre-Conditions:
* `SOURCE_A` with minimal demographic data is registered (external id: `MDM-07A`)
* `MASTER_A` has been established for `SOURCE_A`
* `SOURCE_B` with demographic data similar to `SOURCE_A` is registered (external id: `MDM-07B`)
* `MASTER_B` has been established for `SOURCE_B`
* `SOURCE_B` has a relationship with `MASTER_A` of type `MDM-Duplicate` and class `AUTO`

Test Steps:
* The `IRecordMergeService` instructs that `SOURCE_B` should be merged into `MASTER_A`

Outcomes:
* `SOURCE_A` points to `MASTER_A` with relationship `MDM-Master` and class `AUTO`
* `SOURCE_B` points to `MASTER_A` with relationship `MDM-Master` and class `VERIFIED`
* `SOURCE_B` relationship to `MASTER_A` with relationship `MDM-Duplicate` is removed
* `SOURCE_B` relationship to `MASTER_B` is removed
* `MASTER_B` is obsolete (no longer appears in searches)
* `MASTER_A` points to `MASTER_B` with relationship `REPLACES`
* Querying for `MDM-07A` or `MDM-07B` returns `MASTER_A`

### Case 8: Manual Linking of a Source is "Sticky"

In this test case, we perform the same test steps as Case #5, however becase the link between `SOURCE_B` and `MASTER_A` has a link type of `VERIFIED` the relationship
does not change.

Pre-Steps:
* `SOURCE_A` with minimal demographic data is registered (external id: `MDM-08A`)
* `MASTER_A` has been established for `SOURCE_A` with relationship `MDM-Master` and class `AUTO`
* `SOURCE_B` with demographic data similar to `SOURCE_A` is registered (external id: `MDM-08B`)
* `SOURCE_B` has a relationship with `MASTER_A` of type `MDM-Master` and class `VERIFIED`

Test Steps:
* `SOURCE_B` is updated such that the data is wildly different than previously estalbished

Outcomes: 
* `SOURCE_B` remains linked to `MASTER_A` with `MDM-Master` and class `VERIFIED`
* `SOURCE_A` id detached from `MASTER_A` and a new master `MASTER_B` is established
* `SOURCE_A` is linked to `MASTER_B` with link type `MDM-Master` and class `AUTO`
* `SOURCE_A` is related to `MASTER_A` with link type `MDM-OriginalMaster` and class `AUTO`
* Searching for `MDM-08B` returns `MASTER_A`
* Searching for `MDM-08A` returns `MASTER_B`

### Case 9: Ignoring of Candidate Records

In this test case, we want to flag an identified candidate link as an ignore condition. Upon subsequent updates, the source should never be considered for matching
or merging with another master, even if they meet criteria for candidate linking.

Pre-Steps:
* `SOURCE_A` with minimal demographic data is registered (external id: `MDM-09A`)
* `MASTER_A` has been established for `SOURCE_A` with relationship `MDM-Master` and class `AUTO`
* `SOURCE_B` with demographic data similar to `SOURCE_A` is registered (external id: `MDM-09B`)
* `MASTER_B` has been established for `SOURCE_B` with relationship `MDM-Master` and class `AUTO`
* `SOURCE_B` has a link to `MASTER_A` with relationship `MDM-Duplicate` and class `AUTO`

Test Steps A:
* The `IRecordMergeService` is called to ignore the relationship between `SOURCE_B` and `MASTER_A`

Outcome A:
* `SOURCE_B` is related to `MASTER_A` with relationship `MDM-IgnoreCandidateLocal` and class `VERIFIED`

Test Steps B:
* Update `SOURCE_B` such that the demographics information matches `SOURCE_A`
* Save `SOURCE_B`

Outcome B:
* No changes in `SOURCE_B`'s relationship with `MASTER_A` or `MASTER_B`

### Case 10: Un-Merge/Detach of Source from a Master is "Sticky"

In this test case, we ensure that when a source is detached/unmerged from a master, that the detached source is never re-attached to the original master, even 
if it matches according to the normal rewrite rules.

Pre-Steps:
* `SOURCE_A` with minimal demographic data is registered (external id: `MDM-10A`)
* `MASTER_A` has been established for `SOURCE_A` with relationship `MDM-Master` and class `AUTO`
* `SOURCE_B` with demographic data identical to `SOURCE_A` is registered (external id: `MDM-10B`)
* `SOURCE_B` has a link to `MASTER_A` with relationship `MDM-Master` and class `AUTO`

Test Steps A:
* Use the `IRecordMergingService` to Unmerge `SOURCE_B` from `MASTER_A`

Outcome A:
* `MASTER_B` should be established for `SOURCE_B` with relationship `MDM-Master` and class `VERIFIED`
* `SOURCE_B` has a relationship with `MASTER_A` with relationship `MDM-OriginalMaster` and class `VERIFIED`
* Querying for `MDM-09A` returns `MASTER_A` and `MDM-09B` returns `MASTER_B`

Test Steps B:
* Update `SOURCE_B` so that the dmeographics exactly match `SOURCE_A`
* Save `SOURCE_B`

Outcome B:
* `SOURCE_B` remains linked to `MASTER_B` with `MDM-Master` and class `VERIFIED`

### Test Case 11: Create a Record of Truth

In this test case we establish a known record of truth. Records of truth are special records which contain the most accurate information about a MASTER record.

### Test Case 12: Update Record of Truth 

In this test case we ensure that the Record Of Truth is not moved or merged in any fashion (similar to other locals). We also ensure that the synthesization of results take information from the ROT.

### Test Case 13: Sensitive Data is Removed

In this test case we setup two different policy levels on our source information.

* `SOURCE_A` -> No policies applied
* `SOURCE_B` -> TABOO policy applied

We then link `SOURCE_A` and `SOURCE_B` to a single `MASTER_A`. Upon querying for data from `MASTER_A` with a principal which has no access to `TABOO` we note that only the data from `SOURCE_A` is included in the result set. We then change our prinicipal to one where `TABOO` is permitted and re-query. We should note that information from `SOURCE_B` is updated.

### Test Case 14: Update to MASTER is Redirected To Local

In SanteDB no system is permitted to operate on a master record without appropriate policies in place. This test case will test a condition where a client mistakenly 
attempts to direclty update the master record from the API. The master record should realize this condition and should redirect the updates to the LOCAL record
which the caller created in a previous step.

### Test Case 15: Update to MASTER results in new LOCAL

This is a special case of Test Case #13 whereby a new system has "downloaded" the master record and is attempting to re-submit the master. Here the MDM layer should
establish a new SOURCE record for the update, and segregate (protect) the master/golden record.

### Test Case 16: MASTER<>MASTER Merging

This test case is a requirement for PMIR, we will attempt as a foreign credential to instruct the MDM layer to MERGE two master records together. The foreign credential
which has no permission to MDM Write Master should result in an error/policy violation. When re-authenticating as a credential which has appropriate permission to administer
MDM Masters, the two masters and their source records should be merged according to the logic specified.

### Test Case 17: LOCAL>LOCAL Merging

This test case a LOCAL source for records `SOURCE_A` and `SOURCE_B` wishes to merge `SOURCE_A` into `SOURCE_B`. The serouce has control of the two records and therefore the merge is committed.

Pre-Steps:
* `SOURCE_A` is registered with master link to `MASTER_A` (id: `MDM-17A`)
* `SOURCE_B` is registered with master link to `MASTER_B` (id: `MDM-17B`)

Test Steps:
* The `IRecordMergeService` is called to indicate `SOURCE_A` replaces `SOURCE_B`

The MDM layer should perform the appropriate merging logic.

Outcomes:
* `SOURCE_A` is related to `SOURCE_B` with relationship `REPLACES`
* `SOURCE_B` is marked as `OBSOLETE`
* `SOURCE_A` contains both `MDM-17A` and `MDM-17B` links

### Test Case 18: LOCAL>LOCAL Merging Cross Domain

This test case ensures that a non-owner source of a LOCAL record cannot merge two source records with different ownership. 

Pre-Steps:
* Authenticated as MDM-17A identity
* `SOURCE_A` is registered with `MASTER_A` (id: `MDM-18A`)
* `SOURCE_B` is registered with `MASTER_B` (id: `MDM-18B`)

Test Steps:
* Authenticate as MDM-17B
* Use `IRecordMergeService` in an attempt to merge `SOURCE_A` into `SOURCE_B`

Outcomes:
* An error is thrown indicating lack of ownership
* `SOURCE_A` and `SOURCE_B` remain unchanged, linked to `MASTER_A` and `MASTER_B` respectively

### Test Case 19: Update Relationship Directly

In this test case, an API caller attempts to modify the MDM relationships directly and receives various behaviors related to the process:

Pre-Steps:
* `SOURCE_A` is registered with `MASTER_A` (id: `MDM-19A`)
* `SOURCE_B` is registered with `MASTER_A` (same demographics, id `MDM-19B`)
* `SOURCE_C` is registered with `MASTER_B` (id: `MDM-19C`)
* `SOURCE_D` is registered with `MASTER_C` and candidate link to `MASTER_B` (id: `MDM-19D`)

Test Steps:
* Directly call update to `EntityRelationship` in attempt to `DELETE` link between `SOURCE_A` and `MASTER_A`
* Directly call update in attempt to UPDATE link between `SOURCE_A

### Test Case 20: Appropriate Policy Blocks in Place

This test case ensures that a principal with access to TABOO Data is properly disclosed data in records marked as TABOO.

### Test Case 21: Processing of Bundle Preserves Transaction

This test case ensures that the MDM layer processing bundles preserves the existing bundle transaction components. The test will ensure that a bundle with a Patient and Organization are properly stored and retrieved from the underlying MDM persistence layer.

