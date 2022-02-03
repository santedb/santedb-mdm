using SanteDB.Core.Security.Services;
using SanteDB.Core;
using SanteDB.Core.Model;
using SanteDB.Core.Model.Collection;
using SanteDB.Core.Model.Constants;
using SanteDB.Core.Model.DataTypes;
using SanteDB.Core.Model.Entities;
using SanteDB.Core.Model.Roles;
using SanteDB.Core.Model.Security;
using SanteDB.Core.Security;
using SanteDB.Core.Services;
using SanteDB.Persistence.MDM.Services;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using SanteDB.Core.TestFramework;
using SanteDB.Core.Security.Claims;
using SanteDB.Core.Services.Impl;
using NUnit.Framework;
using SanteDB.Caching.Memory.Configuration;

namespace SanteDB.Persistence.MDM.Test
{
    /// <summary>
    /// Tests the MDM daemon service for testing capabilities
    /// </summary>
    [ExcludeFromCodeCoverage]
    [TestFixture(Category = "Master Data Management")]
    public class MdmAssociationTest : DataTest
    {
        // Test authority
        private readonly AssigningAuthority m_testAuthority = new AssigningAuthority("TEST-MDM", "TEST-MDM", "1.2.3.4.9999") { Key = Guid.NewGuid() };

        private IRepositoryService<Patient> m_patientRepository;
        private IRepositoryService<Person> m_personRepository;
        private IRecordMergingService<Patient> m_patientMerge;
        private IRepositoryService<Entity> m_entityRepository;

        /// <summary>
        /// Setup the test class
        /// </summary>
        [SetUp]
        public void ClassInitialize()
        {
            typeof(MemoryCacheConfigurationSection).Equals((null)); // Force load
            typeof(MdmDataManagementService).Equals(null); // Trick - Force test context to load
            TestApplicationContext.TestAssembly = typeof(MdmAssociationTest).Assembly;
            TestApplicationContext.Initialize(TestContext.CurrentContext.TestDirectory);
            ApplicationServiceContext.Current.AddBusinessRule(typeof(BundleBusinessRule));
            ApplicationServiceContext.Current.AddBusinessRule(typeof(NationalHealthIdRule));
            this.m_patientRepository = ApplicationServiceContext.Current.GetService<IRepositoryService<Patient>>();
            this.m_personRepository = ApplicationServiceContext.Current.GetService<IRepositoryService<Person>>();
            this.m_patientMerge = ApplicationServiceContext.Current.GetService<IRecordMergingService<Patient>>();
            this.m_entityRepository = ApplicationServiceContext.Current.GetService<IRepositoryService<Entity>>();
        }

        /// <summary>
        /// This test registers a new patient and ensures that parent repository calls are intercepted
        /// </summary>
        // TODO: This test should only be enabled if we decide to call upstream repositories to have masters
        // [Test(Description = "Case 0: Chained / Upstream Calls")]
        public void TestMdmInterceptsParentTypes()
        {
            using (AuthenticationContext.EnterSystemContext())
            {
                var patientUnderTest = new Patient()
                {
                    DateOfBirth = DateTime.Parse("1983-01-10"),
                    Names = new List<EntityName>()
                {
                    new EntityName(NameUseKeys.Legal, "MDM", "Test Subject 0")
                },
                    Identifiers = new List<EntityIdentifier>()
                {
                    new EntityIdentifier(this.m_testAuthority, "MDM-00")
                },
                    GenderConceptKey = Guid.Parse("F4E3A6BB-612E-46B2-9F77-FF844D971198")
                };

                var savedLocal = this.m_patientRepository.Insert(patientUnderTest);

                // Assert -> A local is returned from the patient repository with a key and version id
                Assert.IsNotNull(savedLocal.Key);
                Assert.IsNotNull(savedLocal.VersionKey);

                // Assert -> A query by identifier should result in a master being returned
                var queriedMaster = this.m_patientRepository.Find(o => o.Identifiers.Any(i => i.Value == "MDM-00")).FirstOrDefault();
                Assert.AreEqual("M", queriedMaster.GetTag("$mdm.type"));
                Assert.AreEqual("true", queriedMaster.GetTag("$generated"));
                Assert.AreEqual(1, queriedMaster.Names.Count);
                Assert.AreEqual(2, queriedMaster.Identifiers.Count);

                // Assert -> A fetch by ID from master should return master
                var fetchMaster = this.m_patientRepository.Get(queriedMaster.Key.Value);
                Assert.AreEqual(queriedMaster.Key, fetchMaster.Key);

                // Assert -> A fetch by ID for local should return local
                var fetchLocal = this.m_patientRepository.Get(savedLocal.Key.Value);
                Assert.AreEqual(1, fetchLocal.Names.Count);
                Assert.AreEqual(1, fetchLocal.Identifiers.Count);
                Assert.IsNull(fetchLocal.GetTag("$mdm.type"));

                // Assert -> Call to IRepositoryService<Person> behaves the same way
                var queriedPersonMaster = this.m_personRepository.Find(o => o.Identifiers.Any(i => i.Value == "MDM-00")).FirstOrDefault();
                Assert.AreEqual("M", queriedMaster.GetTag("$mdm.type"));
                Assert.AreEqual("true", queriedMaster.GetTag("$generated"));
                Assert.AreEqual(1, queriedMaster.Names.Count);
                Assert.AreEqual(2, queriedMaster.Identifiers.Count);

                // Assert -> Call to IRepositoryService<Entity> behaves the same way
                var queryEntityMaster = this.m_entityRepository.Find(o => o.Identifiers.Any(i => i.Value == "MDM-00")).FirstOrDefault();
                Assert.AreEqual("M", queriedMaster.GetTag("$mdm.type"));
                Assert.AreEqual("true", queriedMaster.GetTag("$generated"));
                Assert.AreEqual(1, queriedMaster.Names.Count);
                Assert.AreEqual(2, queriedMaster.Identifiers.Count);
            }
        }

        /// <summary>
        /// This test registers a new patient in the database and ensures that the MDM layer established as new master
        /// </summary>
        /// <remarks>
        /// The MDM behavior should be as follows:
        /// 1. New Patient with no matches
        /// 2. MDM Master is established
        /// 3. Local (submitted patient) is linked to Master
        /// 4. NHID Generator fires
        /// </remarks>
        [Test(Description = "Case 1: Register New Record")]
        public void TestMdmShouldCreateNewMaster()
        {
            using (AuthenticationContext.EnterSystemContext())
            {
                var patientUnderTest = new Patient()
                {
                    DateOfBirth = DateTime.Parse("1983-01-10"),
                    Names = new List<EntityName>()
                {
                    new EntityName(NameUseKeys.Legal, "MDM", "Test Subject 1")
                },
                    Identifiers = new List<EntityIdentifier>()
                {
                    new EntityIdentifier(this.m_testAuthority, "MDM-01")
                },
                    GenderConceptKey = Guid.Parse("F4E3A6BB-612E-46B2-9F77-FF844D971198")
                };

                var savedLocal = this.m_patientRepository.Insert(patientUnderTest);

                // Assert -> A local is returned from the patient repository with a key and version id
                Assert.IsNotNull(savedLocal.Key);
                Assert.IsNotNull(savedLocal.VersionKey);

                // Assert -> A query by identifier should result in a master being returned
                var queriedMaster = this.m_patientRepository.Find(o => o.Identifiers.Any(i => i.Value == "MDM-01")).FirstOrDefault();
                Assert.AreEqual("M", queriedMaster.GetTag("$mdm.type"));
                Assert.AreEqual("true", queriedMaster.GetTag("$generated"));
                Assert.AreEqual(1, queriedMaster.Names.Count);
                Assert.AreEqual(2, queriedMaster.Identifiers.Count);

                // Assert -> A fetch by ID from master should return master
                var fetchMaster = this.m_patientRepository.Get(queriedMaster.Key.Value);
                Assert.AreEqual(queriedMaster.Key, fetchMaster.Key);

                // Assert -> A fetch by ID for local should return local
                var fetchLocal = this.m_patientRepository.Get(savedLocal.Key.Value);
                Assert.AreEqual(1, fetchLocal.Names.Count);
                Assert.AreEqual(1, fetchLocal.Identifiers.Count);
                Assert.IsNull(fetchLocal.GetTag("$mdm.type"));
            }
        }

        /// <summary>
        /// Tests that the MDM should link two SOURCE records which are identical according to the matcher
        /// </summary>
        [Test(Description = "Case 2: Register Duplicate Record")]
        public void TestMdmShouldAutoLinkCandidate()
        {
            using (AuthenticationContext.EnterSystemContext())
            {
                var patientUnderTestA = new Patient()
                {
                    DateOfBirth = DateTime.Parse("1983-02-10"),
                    Names = new List<EntityName>()
                {
                    new EntityName(NameUseKeys.Legal, "MDM", "Test Subject 2")
                },
                    Identifiers = new List<EntityIdentifier>()
                {
                    new EntityIdentifier(this.m_testAuthority, "MDM-02A")
                },
                    GenderConceptKey = Guid.Parse("F4E3A6BB-612E-46B2-9F77-FF844D971198"),
                    MultipleBirthOrder = 0
                };

                var savedLocalA = this.m_patientRepository.Insert(patientUnderTestA);

                // Assert -> A local is returned from the patient repository with a key and version id
                Assert.IsNotNull(savedLocalA.Key);
                Assert.IsNotNull(savedLocalA.VersionKey);

                // Assert -> A query by identifier should result in a master being returned
                var queriedMaster = this.m_patientRepository.Find(o => o.Identifiers.Any(i => i.Value == "MDM-02A")).FirstOrDefault();
                Assert.AreEqual("M", queriedMaster.GetTag("$mdm.type"));
                Assert.AreEqual("true", queriedMaster.GetTag("$generated"));
                Assert.AreEqual(1, queriedMaster.Names.Count);
                Assert.AreEqual(2, queriedMaster.Identifiers.Count);

                // Assert -> A fetch by ID from master should return master
                var fetchMaster = this.m_patientRepository.Get(queriedMaster.Key.Value);
                Assert.AreEqual(queriedMaster.Key, fetchMaster.Key);

                var patientUnderTestB = new Patient()
                {
                    DateOfBirth = DateTime.Parse("1983-02-10"),
                    Names = new List<EntityName>()
                {
                    new EntityName(NameUseKeys.Legal, "MDM", "Test Subject 2")
                },
                    Identifiers = new List<EntityIdentifier>()
                {
                    new EntityIdentifier(this.m_testAuthority, "MDM-02B")
                },
                    GenderConceptKey = Guid.Parse("F4E3A6BB-612E-46B2-9F77-FF844D971198"),
                    MultipleBirthOrder = 0
                };

                var savedLocalB = this.m_patientRepository.Insert(patientUnderTestB);

                // Assert -> A local is returned from the patient repository with a key and version id
                Assert.IsNotNull(savedLocalB.Key);
                Assert.IsNotNull(savedLocalB.VersionKey);

                // Assert -> A query by identifier should result in a master being returned
                queriedMaster = this.m_patientRepository.Find(o => o.Identifiers.Any(i => i.Value == "MDM-02B")).FirstOrDefault();
                Assert.AreEqual("M", queriedMaster.GetTag("$mdm.type"));
                Assert.AreEqual("true", queriedMaster.GetTag("$generated"));
                Assert.AreEqual(1, queriedMaster.Names.Count);
                Assert.AreEqual(3, queriedMaster.Identifiers.Count);
                Assert.IsTrue(queriedMaster.Identifiers.Any(o => o.Value == "MDM-02A"));
                Assert.IsTrue(queriedMaster.Identifiers.Any(o => o.Value == "MDM-02B"));
                Assert.IsTrue(queriedMaster.Relationships.Any(o => o.SourceEntityKey == savedLocalA.Key));
                Assert.IsTrue(queriedMaster.Relationships.Any(o => o.SourceEntityKey == savedLocalB.Key));
            }
        }

        /// <summary>
        /// Test that when a probable match occurs masters and links are created
        /// </summary>
        /// <remarks>
        /// At the end of this test, the database should have:
        /// 1. A MASTER_A for SOURCE_A with SOURCE_A pointing at MASTER_A
        /// 2. A MASTER_B for SOURCE_B with SOURCE_B pointing at MASTER_B
        /// 3. SOURCE_B pointing at MASTER_A with candidate link
        /// </remarks>
        [Test(Description = "Case 3: Register Suspected Duplicate Record")]
        public void TestMdmShouldLinkCandidateRecord()
        {
            using (AuthenticationContext.EnterSystemContext())
            {
                var patientUnderTestA = new Patient()
                {
                    DateOfBirth = DateTime.Parse("1983-03-10"),
                    Names = new List<EntityName>()
                {
                    new EntityName(NameUseKeys.Legal, "MDM", "Test Subject 3")
                },
                    Identifiers = new List<EntityIdentifier>()
                {
                    new EntityIdentifier(this.m_testAuthority, "MDM-03A")
                },
                    GenderConceptKey = Guid.Parse("F4E3A6BB-612E-46B2-9F77-FF844D971198"),
                    MultipleBirthOrder = 1
                };

                var savedLocalA = this.m_patientRepository.Insert(patientUnderTestA);

                // Assert -> A local is returned from the patient repository with a key and version id
                Assert.IsNotNull(savedLocalA.Key);
                Assert.IsNotNull(savedLocalA.VersionKey);

                // Assert -> A query by identifier should result in a master being returned
                var queriedMasterA = this.m_patientRepository.Find(o => o.Identifiers.Any(i => i.Value == "MDM-03A")).FirstOrDefault();
                Assert.AreEqual("M", queriedMasterA.GetTag("$mdm.type"));
                Assert.AreEqual("true", queriedMasterA.GetTag("$generated"));
                Assert.AreEqual(1, queriedMasterA.Names.Count);
                Assert.AreEqual(2, queriedMasterA.Identifiers.Count);
                Assert.IsTrue(queriedMasterA.Relationships.Any(o => o.SourceEntityKey == savedLocalA.Key && o.RelationshipTypeKey == MdmConstants.MasterRecordRelationship));

                // Assert -> A fetch by ID from master should return master
                var fetchMaster = this.m_patientRepository.Get(queriedMasterA.Key.Value);
                Assert.AreEqual(queriedMasterA.Key, fetchMaster.Key);

                var patientUnderTestB = new Patient()
                {
                    DateOfBirth = DateTime.Parse("1983-03-10"),
                    Names = new List<EntityName>()
                {
                    new EntityName(NameUseKeys.Legal, "MDM", "Test Subject 3")
                },
                    Identifiers = new List<EntityIdentifier>()
                {
                    new EntityIdentifier(this.m_testAuthority, "MDM-03B")
                },
                    GenderConceptKey = Guid.Parse("F4E3A6BB-612E-46B2-9F77-FF844D971198"),
                    MultipleBirthOrder = 2
                };

                var savedLocalB = this.m_patientRepository.Insert(patientUnderTestB);

                // Assert -> A local is returned from the patient repository with a key and version id
                Assert.IsNotNull(savedLocalB.Key);
                Assert.IsNotNull(savedLocalB.VersionKey);

                // Assert -> A query by identifier should result in a master being returned
                var queriedMasterB = this.m_patientRepository.Find(o => o.Identifiers.Any(i => i.Value == "MDM-03B")).FirstOrDefault();
                Assert.AreEqual("M", queriedMasterB.GetTag("$mdm.type"));
                Assert.AreEqual("true", queriedMasterB.GetTag("$generated"));
                Assert.AreEqual(1, queriedMasterB.Names.Count);
                Assert.AreEqual(2, queriedMasterB.Identifiers.Count);
                Assert.IsTrue(queriedMasterB.Relationships.Any(o => o.SourceEntityKey == savedLocalB.Key && o.RelationshipTypeKey == MdmConstants.MasterRecordRelationship));
                // A points at B
                Assert.IsTrue(queriedMasterB.Relationships.Any(o => o.SourceEntityKey == savedLocalB.Key && o.RelationshipTypeKey == MdmConstants.CandidateLocalRelationship && o.TargetEntityKey == queriedMasterA.Key));
                Assert.AreEqual(MdmConstants.AutomagicClassification, queriedMasterB.Relationships.First(o => o.SourceEntityKey == savedLocalB.Key && o.RelationshipTypeKey == MdmConstants.CandidateLocalRelationship && o.TargetEntityKey == queriedMasterA.Key).ClassificationKey);
            }
        }

        /// <summary>
        /// When updating a record with one local there should not be a new master created on the update of the record
        /// </summary>
        [Test(Description = "Case 4: Update Maintains Master Link")]
        public void TestMdmUpdateShouldNotCreateNewMaster()
        {
            using (AuthenticationContext.EnterSystemContext())
            {
                var patientUnderTest = new Patient()
                {
                    DateOfBirth = DateTime.Parse("1983-04-10"),
                    Names = new List<EntityName>()
                {
                    new EntityName(NameUseKeys.Legal, "MDM", "Test Subject 4")
                },
                    Identifiers = new List<EntityIdentifier>()
                {
                    new EntityIdentifier(this.m_testAuthority, "MDM-04")
                },
                    GenderConceptKey = Guid.Parse("F4E3A6BB-612E-46B2-9F77-FF844D971198")
                };

                var savedLocal = this.m_patientRepository.Insert(patientUnderTest);

                // Assert -> A local is returned from the patient repository with a key and version id
                Assert.IsNotNull(savedLocal.Key);
                Assert.IsNotNull(savedLocal.VersionKey);

                // Assert -> A query by identifier should result in a master being returned
                var queriedMaster = this.m_patientRepository.Find(o => o.Identifiers.Any(i => i.Value == "MDM-04")).FirstOrDefault();
                Assert.AreEqual("M", queriedMaster.GetTag("$mdm.type"));
                Assert.AreEqual("true", queriedMaster.GetTag("$generated"));
                Assert.AreEqual(1, queriedMaster.Names.Count);
                Assert.AreEqual(2, queriedMaster.Identifiers.Count);
                Assert.AreEqual(patientUnderTest.DateOfBirth, queriedMaster.DateOfBirth);

                // Update the local and save
                savedLocal.DateOfBirth = DateTime.Parse("1984-04-11");
                savedLocal.Tags.Clear();
                savedLocal = this.m_patientRepository.Save(savedLocal);
                Assert.AreEqual(queriedMaster.Key, savedLocal.Relationships.FirstOrDefault(o => o.RelationshipTypeKey == MdmConstants.MasterRecordRelationship)?.TargetEntityKey);
                var afterUpdateMaster = this.m_patientRepository.Find(o => o.Identifiers.Any(i => i.Value == "MDM-04")).FirstOrDefault();
                Assert.AreEqual(afterUpdateMaster.Key, queriedMaster.Key);
                Assert.AreNotEqual(queriedMaster.DateOfBirth, afterUpdateMaster.DateOfBirth);
            }
        }

        /// <summary>
        /// Test that when a flagged probable match is updated to match the master it is linked
        /// </summary>
        /// <remarks>
        /// Midway the test database will have a structure like:
        /// 1. A MASTER_A for SOURCE_A with SOURCE_A pointing at MASTER_A
        /// 2. A MASTER_B for SOURCE_B with SOURCE_B pointing at MASTER_B
        /// 3. SOURCE_B pointing at MASTER_A with candidate link
        ///
        /// The test then updates the multiplebirthorder to match and saves SOURCE_B
        /// This means that SOURCE_B should indicate a match with MASTER_A and:
        /// 1. MASTER_B should be marked obsolete
        /// 2. MASTER_A should be marked as replacing MASTER_B
        /// 3. SOURCE_B should be marked as MSATER_A source
        /// 4. SOURCE_B should have an ORIGINAL link to MASTER_B
        /// </remarks>
        [Test(Description = "Case 5: Update to match performs auto-link")]
        public void TestMdmCandidateUpdateToMatchMerges()
        {
            using (AuthenticationContext.EnterSystemContext())
            {
                var patientUnderTestA = new Patient()
                {
                    DateOfBirth = DateTime.Parse("1983-05-10"),
                    Names = new List<EntityName>()
                {
                    new EntityName(NameUseKeys.Legal, "MDM", "Test Subject 5")
                },
                    Identifiers = new List<EntityIdentifier>()
                {
                    new EntityIdentifier(this.m_testAuthority, "MDM-05A")
                },
                    GenderConceptKey = Guid.Parse("F4E3A6BB-612E-46B2-9F77-FF844D971198"),
                    MultipleBirthOrder = 1
                };

                var savedLocalA = this.m_patientRepository.Insert(patientUnderTestA);

                // Assert -> A local is returned from the patient repository with a key and version id
                Assert.IsNotNull(savedLocalA.Key);
                Assert.IsNotNull(savedLocalA.VersionKey);

                // Assert -> A query by identifier should result in a master being returned
                var queriedMasterA = this.m_patientRepository.Find(o => o.Identifiers.Any(i => i.Value == "MDM-05A")).FirstOrDefault();
                Assert.AreEqual("M", queriedMasterA.GetTag("$mdm.type"));
                Assert.AreEqual("true", queriedMasterA.GetTag("$generated"));
                Assert.AreEqual(1, queriedMasterA.Names.Count);
                Assert.AreEqual(2, queriedMasterA.Identifiers.Count);

                var patientUnderTestB = new Patient()
                {
                    DateOfBirth = DateTime.Parse("1983-05-10"),
                    Names = new List<EntityName>()
                {
                    new EntityName(NameUseKeys.Legal, "MDM", "Test Subject 5")
                },
                    Identifiers = new List<EntityIdentifier>()
                {
                    new EntityIdentifier(this.m_testAuthority, "MDM-05B")
                },
                    GenderConceptKey = Guid.Parse("F4E3A6BB-612E-46B2-9F77-FF844D971198"),
                    MultipleBirthOrder = 2
                };

                var savedLocalB = this.m_patientRepository.Insert(patientUnderTestB);

                // Assert -> A local is returned from the patient repository with a key and version id
                Assert.IsNotNull(savedLocalB.Key);
                Assert.IsNotNull(savedLocalB.VersionKey);

                // Assert -> A query by identifier should result in a master being returned
                var queriedMasterB = this.m_patientRepository.Find(o => o.Identifiers.Any(i => i.Value == "MDM-05B")).FirstOrDefault();
                Assert.AreEqual("M", queriedMasterB.GetTag("$mdm.type"));
                Assert.AreEqual("true", queriedMasterB.GetTag("$generated"));
                Assert.AreEqual(1, queriedMasterB.Names.Count);
                Assert.AreEqual(2, queriedMasterB.Identifiers.Count);
                Assert.IsTrue(queriedMasterB.Relationships.Any(o => o.SourceEntityKey == savedLocalB.Key && o.RelationshipTypeKey == MdmConstants.MasterRecordRelationship));
                // A points at B as a candidate
                Assert.IsTrue(queriedMasterB.Relationships.Any(o => o.SourceEntityKey == savedLocalB.Key && o.RelationshipTypeKey == MdmConstants.CandidateLocalRelationship && o.TargetEntityKey == queriedMasterA.Key));

                // Let's update localB
                savedLocalB.MultipleBirthOrder = savedLocalA.MultipleBirthOrder;
                savedLocalB.Tags.Clear();
                var afterUpdateSavedLocalB = this.m_patientRepository.Save(savedLocalB);
                // Now the relationships should be updated so MASTER_A is returned
                var queriedMasterAfterUpdate = this.m_patientRepository.Find(o => o.Identifiers.Any(i => i.Value == "MDM-05B")).SingleOrDefault();
                Assert.AreEqual(queriedMasterA.Key, queriedMasterAfterUpdate.Key);
                Assert.IsTrue(queriedMasterAfterUpdate.Relationships.Any(o => o.SourceEntityKey == savedLocalA.Key && o.RelationshipTypeKey == MdmConstants.MasterRecordRelationship));
                Assert.IsTrue(queriedMasterAfterUpdate.Relationships.Any(o => o.SourceEntityKey == savedLocalB.Key && o.RelationshipTypeKey == MdmConstants.MasterRecordRelationship));

                // Queried MasterAfterUpdate should point to queriedMasterB as replaces
                Assert.IsTrue(queriedMasterAfterUpdate.Relationships.Any(o => o.RelationshipTypeKey == EntityRelationshipTypeKeys.Replaces && o.TargetEntityKey == queriedMasterB.Key));

                // Fetch MASTER B and check status
                var masterId = queriedMasterB.Key.Value;
                var masterRaw = this.m_entityRepository.Get(masterId);
                Assert.AreEqual(StatusKeys.Inactive, masterRaw.StatusConceptKey);
            }
        }

        /// <summary>
        /// Tests that an MDM SOURCE record is detached automatically when it no longer matches the master it is attached to
        /// </summary>
        /// <remarks>
        /// Midway through this test there is the following data:
        /// 1. SOURCE_A pointing at MASTER_A
        /// 2. SOURCE_B pointing at MASTER_A
        /// Then, we update SOURCE_B so that it no longer matches MASTER_A we expect the following behaviors:
        /// 1. SOURCE_A pointing at MASTER_A
        /// 2. SOURCE_B point at MASTER_B
        /// 3. SOURCE_B has pointer to MASTER_A indicating the original relationship
        /// </remarks>
        [Test(Description = "Case 6: Update auto-established master to not match")]
        public void TestMdmMultipleSourcesNoLongerMatchDetach()
        {
            using (AuthenticationContext.EnterSystemContext())
            {
                var patientUnderTestA = new Patient()
                {
                    DateOfBirth = DateTime.Parse("1983-06-10"),
                    Names = new List<EntityName>()
                {
                    new EntityName(NameUseKeys.Legal, "MDM", "Test Subject 6")
                },
                    Identifiers = new List<EntityIdentifier>()
                {
                    new EntityIdentifier(this.m_testAuthority, "MDM-06A")
                },
                    GenderConceptKey = Guid.Parse("F4E3A6BB-612E-46B2-9F77-FF844D971198"),
                    MultipleBirthOrder = 0
                };

                var savedLocalA = this.m_patientRepository.Insert(patientUnderTestA);

                var patientUnderTestB = new Patient()
                {
                    DateOfBirth = DateTime.Parse("1983-06-10"),
                    Names = new List<EntityName>()
                {
                    new EntityName(NameUseKeys.Legal, "MDM", "Test Subject 6")
                },
                    Identifiers = new List<EntityIdentifier>()
                {
                    new EntityIdentifier(this.m_testAuthority, "MDM-06B")
                },
                    GenderConceptKey = Guid.Parse("F4E3A6BB-612E-46B2-9F77-FF844D971198"),
                    MultipleBirthOrder = 0
                };

                var savedLocalB = this.m_patientRepository.Insert(patientUnderTestB);

                // Assert -> A query by identifier should result in a master being returned
                var queriedMaster = this.m_patientRepository.Find(o => o.Identifiers.Any(i => i.Value == "MDM-06B")).FirstOrDefault();
                Assert.AreEqual("M", queriedMaster.GetTag("$mdm.type"));
                Assert.AreEqual("true", queriedMaster.GetTag("$generated"));
                Assert.AreEqual(1, queriedMaster.Names.Count);
                Assert.AreEqual(3, queriedMaster.Identifiers.Count);
                Assert.IsTrue(queriedMaster.Identifiers.Any(o => o.Value == "MDM-06A"));
                Assert.IsTrue(queriedMaster.Identifiers.Any(o => o.Value == "MDM-06B"));

                // Now we want to update local B so that it no longer matches
                // the MDM layer should detach
                savedLocalB.MultipleBirthOrder = 1;
                savedLocalB.DateOfBirth = DateTime.Parse("1983-06-15");
                savedLocalB.Tags.Clear();
                savedLocalB = this.m_patientRepository.Save(savedLocalB);

                // Now we want to re-query the master
                var masterA = this.m_patientRepository.Find(o => o.Identifiers.Any(i => i.Value == "MDM-06A")).FirstOrDefault();
                var masterB = this.m_patientRepository.Find(o => o.Identifiers.Any(i => i.Value == "MDM-06B")).FirstOrDefault();

                Assert.AreNotEqual(masterA.Key, masterB.Key);
                Assert.AreEqual("M", masterA.GetTag("$mdm.type"));
                Assert.AreEqual("true", masterA.GetTag("$generated"));
                Assert.AreEqual(1, masterA.Names.Count);
                Assert.AreEqual(2, masterA.Identifiers.Count);
                Assert.IsTrue(masterA.Identifiers.Any(o => o.Value == "MDM-06A"));

                Assert.AreEqual("M", masterB.GetTag("$mdm.type"));
                Assert.AreEqual("true", masterB.GetTag("$generated"));
                Assert.AreEqual(1, masterB.Names.Count);
                Assert.AreEqual(2, masterB.Identifiers.Count);
                Assert.IsTrue(masterB.Identifiers.Any(o => o.Value == "MDM-06B"));

                // Assert there is an original master link
                Assert.IsTrue(masterB.Relationships.Any(r => r.RelationshipTypeKey == MdmConstants.OriginalMasterRelationship));
            }
        }

        /// <summary>
        /// Test that an MDM record with a CANDIDATE relationship when reconciled points to the master
        /// </summary>
        /// <remarks>
        /// Midway state:
        /// 1. SOURCE_A pointing to MASTER_A
        /// 2. SOURCE_B pointing to MASTER_B
        /// 3. SOURCE_B candidate to MASTER_A
        ///
        /// We then reconcile SOURCE_B's candidate to MASTER_A performing the linking operation via the merging interface
        /// doing a SOURCE_B -> MASTER_A merge (which gets turned into a LINK)
        ///
        /// The final state is:
        /// 1. SOURCE_A pointing to MASTER_A
        /// 2. SOURCE_B pointing to MASTER_A with "VALIDATED"
        /// 3. SOURCE_B candidate to MASTER_A is removed
        /// 4. MASTER_B is obsoleted (no longer appears)
        /// </remarks>
        [Test(Description = "Case 7: Manual Reconciliation of Candidate Record")]
        public void TestMdmReconciliation()
        {
            using (AuthenticationContext.EnterSystemContext())
            {
                var patientUnderTestA = new Patient()
                {
                    DateOfBirth = DateTime.Parse("1983-07-10"),
                    Names = new List<EntityName>()
                {
                    new EntityName(NameUseKeys.Legal, "MDM", "Test Subject 7")
                },
                    Identifiers = new List<EntityIdentifier>()
                {
                    new EntityIdentifier(this.m_testAuthority, "MDM-07A")
                },
                    GenderConceptKey = Guid.Parse("F4E3A6BB-612E-46B2-9F77-FF844D971198"),
                    MultipleBirthOrder = 1
                };

                var savedLocalA = this.m_patientRepository.Insert(patientUnderTestA);
                var queriedMasterA = this.m_patientRepository.Find(o => o.Identifiers.Any(i => i.Value == "MDM-07A")).FirstOrDefault();
                Assert.AreEqual("M", queriedMasterA.GetTag("$mdm.type"));

                var patientUnderTestB = new Patient()
                {
                    DateOfBirth = DateTime.Parse("1983-07-10"),
                    Names = new List<EntityName>()
                {
                    new EntityName(NameUseKeys.Legal, "MDM", "Test Subject 7")
                },
                    Identifiers = new List<EntityIdentifier>()
                {
                    new EntityIdentifier(this.m_testAuthority, "MDM-07B")
                },
                    GenderConceptKey = Guid.Parse("F4E3A6BB-612E-46B2-9F77-FF844D971198"),
                    MultipleBirthOrder = 2
                };

                var savedLocalB = this.m_patientRepository.Insert(patientUnderTestB);

                // Assert -> A query by identifier should result in a master being returned
                var queriedMasterB = this.m_patientRepository.Find(o => o.Identifiers.Any(i => i.Value == "MDM-07B")).FirstOrDefault();
                Assert.AreEqual("M", queriedMasterB.GetTag("$mdm.type"));
                Assert.AreEqual("true", queriedMasterB.GetTag("$generated"));
                Assert.AreEqual(1, queriedMasterB.Names.Count);
                Assert.AreEqual(2, queriedMasterB.Identifiers.Count);
                Assert.IsTrue(queriedMasterB.Relationships.Any(o => o.SourceEntityKey == savedLocalB.Key && o.RelationshipTypeKey == MdmConstants.MasterRecordRelationship));
                // A points at B
                Assert.IsTrue(queriedMasterB.Relationships.Any(o => o.SourceEntityKey == savedLocalB.Key && o.RelationshipTypeKey == MdmConstants.CandidateLocalRelationship && o.TargetEntityKey == queriedMasterA.Key));
                Assert.AreEqual(MdmConstants.AutomagicClassification, queriedMasterB.Relationships.First(o => o.SourceEntityKey == savedLocalB.Key && o.RelationshipTypeKey == MdmConstants.CandidateLocalRelationship && o.TargetEntityKey == queriedMasterA.Key).ClassificationKey);

                // Perform LOCAL>MASTER (i.e. we want LOCAL_B to now point into MASTER_A)
                this.m_patientMerge.Merge(queriedMasterA.Key.Value, new Guid[] { savedLocalB.Key.Value });

                // Outcomes
                savedLocalA = this.m_patientRepository.Get(savedLocalA.Key.Value);
                savedLocalB = this.m_patientRepository.Get(savedLocalB.Key.Value);
                queriedMasterA = this.m_patientRepository.Get(queriedMasterA.Key.Value);
                var rawMasterB = this.m_entityRepository.Get(queriedMasterB.Key.Value);
                queriedMasterB = this.m_patientRepository.Get(queriedMasterB.Key.Value);

                // A and B point to A
                Assert.IsTrue(savedLocalA.LoadProperty(o=>o.Relationships).Any(r => r.RelationshipTypeKey == MdmConstants.MasterRecordRelationship && r.TargetEntityKey == queriedMasterA.Key));
                Assert.IsTrue(savedLocalB.LoadProperty(o => o.Relationships).Any(r => r.RelationshipTypeKey == MdmConstants.MasterRecordRelationship && r.TargetEntityKey == queriedMasterA.Key && r.ClassificationKey == MdmConstants.VerifiedClassification));
                Assert.IsFalse(savedLocalB.LoadProperty(o => o.Relationships).Any(r => r.RelationshipTypeKey == MdmConstants.MasterRecordRelationship && r.TargetEntityKey == rawMasterB.Key));
                Assert.IsTrue(queriedMasterA.LoadProperty(o => o.Relationships).Any(r => r.RelationshipTypeKey == EntityRelationshipTypeKeys.Replaces && r.TargetEntityKey == rawMasterB.Key));
                Assert.AreEqual(StatusKeys.Inactive, queriedMasterB.StatusConceptKey); // No longer under MDM (OBSOLETE)
                Assert.AreEqual(StatusKeys.Inactive, rawMasterB.StatusConceptKey);
            }
        }

        /// <summary>
        /// This tests that a reconciled link is STICKY meaning the normal behavior does not apply
        /// </summary>
        /// <remarks>
        /// Midway state:
        /// 1. SOURCE_A pointing to MASTER_A
        /// 2. SOURCE_B pointing to MASTER_B
        /// 3. SOURCE_B candidate to MASTER_A
        ///
        /// We then reconcile SOURCE_B's candidate to MASTER_A performing the linking operation via the merging interface
        /// doing a SOURCE_B -> MASTER_A merge (which gets turned into a LINK)
        ///
        /// The second state is:
        /// 1. SOURCE_A pointing to MASTER_A
        /// 2. SOURCE_B pointing to MASTER_A with "VALIDATED"
        /// 3. SOURCE_B candidate to MASTER_A is removed
        /// 4. SOURCE_B has original master link to MASTER_B
        /// 5. MASTER_B is obsoleted (no longer appears)
        ///
        /// We then update SOURCE_B so it no longer matches MASTER_A however because the link is VERIFIED it is not touched
        ///
        /// The final state is:
        /// 1. SOURCE_A point to MASTER_A
        /// 2. SOURCE_B (with wildly different name) still pointing to MASTER_A
        /// </remarks>
        [Test(Description = "Case 8: Manual Linkage is Sticky")]
        public void TestMdmReconciliationIsSticky()
        {
            using (AuthenticationContext.EnterSystemContext())
            {
                var patientUnderTestA = new Patient()
                {
                    DateOfBirth = DateTime.Parse("1983-08-10"),
                    Names = new List<EntityName>()
                {
                    new EntityName(NameUseKeys.Legal, "MDM", "Test Subject 8")
                },
                    Identifiers = new List<EntityIdentifier>()
                {
                    new EntityIdentifier(this.m_testAuthority, "MDM-08A")
                },
                    GenderConceptKey = Guid.Parse("F4E3A6BB-612E-46B2-9F77-FF844D971198"),
                    MultipleBirthOrder = 1
                };

                var savedLocalA = this.m_patientRepository.Insert(patientUnderTestA);

                // Assert -> A local is returned from the patient repository with a key and version id
                Assert.IsNotNull(savedLocalA.Key);
                Assert.IsNotNull(savedLocalA.VersionKey);

                // Assert -> A query by identifier should result in a master being returned
                var queriedMasterA = this.m_patientRepository.Find(o => o.Identifiers.Any(i => i.Value == "MDM-08A")).FirstOrDefault();
                Assert.AreEqual("M", queriedMasterA.GetTag("$mdm.type"));
                Assert.IsTrue(queriedMasterA.Relationships.Any(o => o.SourceEntityKey == savedLocalA.Key && o.RelationshipTypeKey == MdmConstants.MasterRecordRelationship));

                // Register source b
                var patientUnderTestB = new Patient()
                {
                    DateOfBirth = DateTime.Parse("1983-08-10"),
                    Names = new List<EntityName>()
                {
                    new EntityName(NameUseKeys.Legal, "MDM", "Test Subject 8")
                },
                    Identifiers = new List<EntityIdentifier>()
                {
                    new EntityIdentifier(this.m_testAuthority, "MDM-08B")
                },
                    GenderConceptKey = Guid.Parse("F4E3A6BB-612E-46B2-9F77-FF844D971198"),
                    MultipleBirthOrder = 2
                };

                var savedLocalB = this.m_patientRepository.Insert(patientUnderTestB);

                // Assert -> A local is returned from the patient repository with a key and version id
                Assert.IsNotNull(savedLocalB.Key);
                Assert.IsNotNull(savedLocalB.VersionKey);

                // Assert -> A query by identifier should result in a master being returned
                var queriedMasterB = this.m_patientRepository.Find(o => o.Identifiers.Any(i => i.Value == "MDM-08B")).FirstOrDefault();
                Assert.AreEqual("M", queriedMasterB.GetTag("$mdm.type"));
                Assert.IsTrue(queriedMasterB.Relationships.Any(o => o.SourceEntityKey == savedLocalB.Key && o.RelationshipTypeKey == MdmConstants.MasterRecordRelationship));
                // A points at B
                Assert.IsTrue(queriedMasterB.Relationships.Any(o => o.SourceEntityKey == savedLocalB.Key && o.RelationshipTypeKey == MdmConstants.CandidateLocalRelationship && o.TargetEntityKey == queriedMasterA.Key));
                Assert.AreEqual(MdmConstants.AutomagicClassification, queriedMasterB.Relationships.First(o => o.SourceEntityKey == savedLocalB.Key && o.RelationshipTypeKey == MdmConstants.CandidateLocalRelationship && o.TargetEntityKey == queriedMasterA.Key).ClassificationKey);

                // Estabalish SOURCE_B to MASTER_A with verified
                this.m_patientMerge.Merge(queriedMasterA.Key.Value, new Guid[] { savedLocalB.Key.Value });

                // Ensure merge occurred
                var queriedAfterMerge = this.m_patientRepository.Find(o => o.Identifiers.Any(i => i.Value == "MDM-08B")).SingleOrDefault();
                // Ensure link is verified
                Assert.AreEqual(MdmConstants.VerifiedClassification, queriedAfterMerge.Relationships.First(o => o.SourceEntityKey == savedLocalB.Key && o.RelationshipTypeKey == MdmConstants.MasterRecordRelationship && o.TargetEntityKey == queriedMasterA.Key).ClassificationKey);

                // Now update saved local B wildly
                savedLocalB.Tags.Clear();
                savedLocalB.Names.Clear();
                savedLocalB.Names.Add(new EntityName(NameUseKeys.Alphabetic, "SMITH", "SOMTHING"));
                savedLocalB.DateOfBirth = DateTime.Now;
                savedLocalB.MultipleBirthOrder = 0;
                savedLocalB.Relationships.Clear();
                this.m_patientRepository.Save(savedLocalB);

                // Re-query and check update
                queriedAfterMerge = this.m_patientRepository.Find(o => o.Identifiers.Any(i => i.Value == "MDM-08B")).SingleOrDefault();
                // Local B still points to A
                Assert.AreEqual(MdmConstants.VerifiedClassification, queriedAfterMerge.Relationships.First(o => o.SourceEntityKey == savedLocalB.Key && o.RelationshipTypeKey == MdmConstants.MasterRecordRelationship && o.TargetEntityKey == queriedMasterA.Key).ClassificationKey);
                // Source A is detached and becomes it's own
                queriedMasterB = this.m_patientRepository.Find(o => o.Identifiers.Any(i => i.Value == "MDM-08A")).SingleOrDefault();
                Assert.AreNotEqual(queriedAfterMerge.Key, queriedMasterB.Key);
            }
        }

        /// <summary>
        /// This test verifies the promotion to ROT status
        /// </summary>
        /// <remarks>
        /// The initial state at the midpoint of this test is:
        /// 1. SOURCE_A points to MASTER_A
        ///
        /// We then authenticate as a prinicpal which has EstablishRecordOfTruth permission. We then
        /// save a new patient with the MDM.TAG set to T and a pointer of MDM-MASTER to MASTER_A
        ///
        /// The final state is:
        /// 1. SOURCE_A points to MASTER_A
        /// 2. ROT_A points to MASTER_A
        /// 3. MASTER_A points to ROT_A
        /// 4. Synthesized records contain only properties in ROT_A not in SOURCE_A
        /// </remarks>
        [Test]
        public void TestMdmPromoteRecordOfTruth()
        {
        }

        /// <summary>
        /// This test verifies that updates to a ROT do not impact any of its associations with the MDM record
        /// </summary>
        /// <remarks>
        /// The initial state at the midpoint of this test is:
        /// 1. SOURCE_A points to MASTER_A
        /// 2. ROT_A points to MASTER_A
        /// 3. MASTER_A points to ROT_A
        /// 4. Synthesized records contain only properties in ROT_A not SOURCE_A
        ///
        /// The test then updates ROT_A to change the properties. The final state of this test is:
        /// 1. SOURCE_A points to MASTER_A
        /// 2. ROT_A still points to MASTER_A
        /// 3. MASTER_A still points to ROT_A
        /// 4. Synthesized record now contains properties updated in ROT_A
        /// </remarks>
        [Test]
        public void TestMdmUpdateRecordOfTruth()
        {
        }
    }
}