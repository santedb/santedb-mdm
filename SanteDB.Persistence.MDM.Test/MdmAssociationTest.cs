/*
 * Portions Copyright (C) 2019 - 2020, Fyfe Software Inc. and the SanteSuite Contributors (See NOTICE.md)
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you 
 * may not use this file except in compliance with the License. You may 
 * obtain a copy of the License at 
 * 
 * http://www.apache.org/licenses/LICENSE-2.0 
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the 
 * License for the specific language governing permissions and limitations under 
 * the License.
 * 
 * User: fyfej
 * Date: 2020-2-2
 */
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
using System.Linq;
using SanteDB.Caching.Memory.Configuration;
using SanteDB.Core.TestFramework;
using SanteDB.Core.Security.Claims;
using SanteDB.Core.Services.Impl;
using NUnit.Framework;

namespace SanteDB.Persistence.MDM.Test
{
    /// <summary>
    /// Tests the MDM daemon service for testing capabilities
    /// </summary>
    [TestFixture(Category = "Master Data Management")]
    public class MdmAssociationTest : DataTest
    {

        // Test authority
        private readonly AssigningAuthority m_testAuthority = new AssigningAuthority("TEST-MDM", "TEST-MDM", "1.2.3.4.9999");

        private IRepositoryService<Patient> m_patientRepository;

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
        }

        /// <summary>
        /// Authenticate 
        /// </summary>
        private void AuthenticateAs(String userName, String password)
        {
            var userService  = ApplicationServiceContext.Current.GetService<IIdentityProviderService>();
            var sesPvdService = ApplicationServiceContext.Current.GetService<ISessionProviderService>();
            var sesIdService = ApplicationServiceContext.Current.GetService<ISessionIdentityProviderService>();
            //var session = sesPvdService.Establish(userService.Authenticate(userName, password), "http://localhost", false, null, null, null);
            //AuthenticationContext.Current = new AuthenticationContext(sesIdService.Authenticate(session));
            AuthenticationContext.Current = new AuthenticationContext(userService.Authenticate(userName, password));
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
        [Test]
        public void TestMdmShouldCreateNewMaster()
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

        /// <summary>
        /// Tests that the MDM should link two SOURCE records which are identical according to the matcher
        /// </summary>
        [Test]
        public void TestMdmShouldAutoLinkCandidate()
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

        /// <summary>
        /// Test that when a probable match occurs masters and links are created
        /// </summary>
        /// <remarks>
        /// At the end of this test, the database should have:
        /// 1. A MASTER_A for SOURCE_A with SOURCE_A pointing at MASTER_A
        /// 2. A MASTER_B for SOURCE_B with SOURCE_B pointing at MASTER_B
        /// 3. SOURCE_B pointing at MASTER_A with candidate link
        /// </remarks>
        [Test]
        public void TestMdmShouldLinkCandidateRecord()
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
        }
        
        /// <summary>
        /// When updating a record with one there should not be a new master created
        /// </summary>
        [Test]
        public void TestMdmUpdateShouldNotCreateNewMaster()
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
        [Test]
        public void TestMdmCandidateUpdateToMatchMerges()
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
            queriedMasterB = this.m_patientRepository.Get(queriedMasterB.Key.Value);

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
        [Test]
        public void TestMdmMultipleSourcesNoLongerMatchDetach()
        {

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
        [Test]
        public void TestMdmReconciliation()
        {

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
        [Test]
        public void TestMdmReconciliationIsSticky()
        {

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

        /// <summary>
        /// When updating a local record with one master there should not be a new master created
        /// </summary>
        [Test]
        public void UpdateShouldNotCreateNewMaster()
        {
            var pservice = ApplicationServiceContext.Current.GetService<IRepositoryService<Patient>>();

            var patient1 = new Patient()
            {
                DateOfBirth = DateTime.Parse("1983-05-04"),
                Names = new List<EntityName>()
                {
                    new EntityName(NameUseKeys.Legal, "Smith", "Testy")
                },
                Identifiers = new List<EntityIdentifier>()
                {
                    new EntityIdentifier(new AssigningAuthority("TEST-MDM", "TEST-MDM", "1.2.3.4.999"), "MDM-3")
                },
                GenderConceptKey = Guid.Parse("F4E3A6BB-612E-46B2-9F77-FF844D971198")
            };
            var localPatient1 = pservice.Insert(patient1);

            // Wait for master
            //Thread.Sleep(1000);
            // Assert that a master record was created
            var masterPatientPre = pservice.Find(o => o.Identifiers.Any(i => i.Value == "MDM-3")).SingleOrDefault();
            Assert.IsNotNull(masterPatientPre);
            Assert.AreEqual("MDM-3", masterPatientPre.Identifiers.Last().Value);
            Assert.AreEqual("M", masterPatientPre.Tags.First(o => o.TagKey == "$mdm.type").Value);
            Assert.AreEqual("$mdm.processed", masterPatientPre.Tags.First().TagKey);
            Assert.AreEqual(localPatient1.Key, masterPatientPre.Relationships.First().SourceEntityKey); // Ensure master is pointed properly

            patient1.DateOfBirth = new DateTime(1984, 05, 22);
            patient1.Names.First().Component.First().Value = "Smithie";
            localPatient1 = pservice.Save(patient1);

            // After updating the MASTER should reflect the newest data for the local
            //Thread.Sleep(1000);
            var masterPatientPost = pservice.Find(o => o.Identifiers.Any(i => i.Value == "MDM-3")).SingleOrDefault();
            Assert.AreEqual(masterPatientPre.Key, masterPatientPost.Key);
            Assert.AreEqual("Smithie", masterPatientPost.Names.First().Component.First().Value);
            Assert.AreEqual("1984-05-22", masterPatientPost.DateOfBirth.Value.ToString("yyyy-MM-dd"));
            Assert.AreEqual(1, masterPatientPost.Relationships.Count(r => r.RelationshipTypeKey == MdmConstants.MasterRecordRelationship));
        }

        /// <summary>
        /// Test that a match with more than one master results in a new master and two probable matches
        /// </summary>
        [Test]
        public void TestProbableMatch()
        {
            var pservice = ApplicationServiceContext.Current.GetService<IRepositoryService<Patient>>();
            var patient1 = new Patient()
            {
                DateOfBirth = DateTime.Parse("1984-01-04"),
                Names = new List<EntityName>()
                {
                    new EntityName(NameUseKeys.Legal, "Smith", "Testy")
                },
                Identifiers = new List<EntityIdentifier>()
                {
                    new EntityIdentifier(new AssigningAuthority("TEST-MDM", "TEST-MDM", "1.2.3.4.999"), "MDM-4A")
                },
                GenderConceptKey = Guid.Parse("F4E3A6BB-612E-46B2-9F77-FF844D971198"),
                MultipleBirthOrder = 1
            };
            var localPatient1 = pservice.Insert(patient1);
            var patient2 = new Patient()
            {
                DateOfBirth = DateTime.Parse("1984-01-04"),
                Names = new List<EntityName>()
                {
                    new EntityName(NameUseKeys.Legal, "Smith", "Testy")
                },
                Identifiers = new List<EntityIdentifier>()
                {
                    new EntityIdentifier(new AssigningAuthority("TEST-MDM", "TEST-MDM", "1.2.3.4.999"), "MDM-4B")
                },
                GenderConceptKey = Guid.Parse("F4E3A6BB-612E-46B2-9F77-FF844D971198"),
                MultipleBirthOrder = 2
            };
            var localPatient2 = pservice.Insert(patient2);

            // There should be two masters
            //Thread.Sleep(1000);
            var masters = pservice.Find(o => o.Identifiers.Any(i => i.Value.Contains("MDM-4")));
            Assert.AreEqual(2, masters.Count());

            // Insert a local record that will trigger a MATCH with both local patients
            var patient3 = new Patient()
            {
                DateOfBirth = DateTime.Parse("1984-01-04"),
                Names = new List<EntityName>()
                {
                    new EntityName(NameUseKeys.Legal, "Smith", "Testy")
                },
                Identifiers = new List<EntityIdentifier>()
                {
                    new EntityIdentifier(new AssigningAuthority("TEST-MDM", "TEST-MDM", "1.2.3.4.999"), "MDM-4C")
                },
                GenderConceptKey = Guid.Parse("F4E3A6BB-612E-46B2-9F77-FF844D971198"),
                MultipleBirthOrder = null
            };
            var localPatient3 = pservice.Insert(patient3);
            //Thread.Sleep(1000);

            // The previous insert should result in a new MASTER being created
            masters = pservice.Find(o => o.Identifiers.Any(i => i.Value.Contains("MDM-4")));
            Assert.AreEqual(3, masters.Count());
            var patient3Master = pservice.Find(o => o.Identifiers.Any(i => i.Value == "MDM-4C")).SingleOrDefault();
            Assert.IsNotNull(patient3Master);

            // There should be 2 probables
            Assert.AreEqual(2, patient3Master.Relationships.Count(r => r.RelationshipTypeKey == MdmConstants.CandidateLocalRelationship));
            Assert.AreEqual(2, patient3Master.Relationships.Count(r => r.RelationshipTypeKey == MdmConstants.CandidateLocalRelationship && masters.Any(m => m.Key == r.TargetEntityKey)));

        }

        /// <summary>
        /// Tests that a non-matching local is updated to meet matching criteria. The MDM should mark the new master because it is automerge
        /// </summary>
        [Test]
        public void TestNonMatchUpdatedToMatch()
        {
            var pservice = ApplicationServiceContext.Current.GetService<IRepositoryService<Patient>>();
            var patient1 = new Patient()
            {
                DateOfBirth = DateTime.Parse("1985-01-04"),
                Names = new List<EntityName>()
                {
                    new EntityName(NameUseKeys.Legal, "Smith", "Testy")
                },
                Identifiers = new List<EntityIdentifier>()
                {
                    new EntityIdentifier(new AssigningAuthority("TEST-MDM", "TEST-MDM", "1.2.3.4.5.5"), "MDM-5A")
                },
                GenderConceptKey = Guid.Parse("F4E3A6BB-612E-46B2-9F77-FF844D971198")
            };
            var localPatient1 = pservice.Insert(patient1);
            var patient2 = new Patient()
            {
                DateOfBirth = DateTime.Parse("1985-01-05"),
                Names = new List<EntityName>()
                {
                    new EntityName(NameUseKeys.Legal, "Smith", "Testy")
                },
                Identifiers = new List<EntityIdentifier>()
                {
                    new EntityIdentifier(new AssigningAuthority("TEST-MDM", "TEST-MDM", "1.2.3.4.5.6"), "MDM-5B")
                },
                GenderConceptKey = Guid.Parse("F4E3A6BB-612E-46B2-9F77-FF844D971198")
            };
            var localPatient2 = pservice.Insert(patient2);

            // There should be two masters
            //Thread.Sleep(1000);
            var masters = pservice.Find(o => o.Identifiers.Any(i => i.Value.Contains("MDM-5")));
            Assert.AreEqual(2, masters.Count());

            // Now update patient 2 
            patient2.DateOfBirth = new DateTime(1985, 01, 04);
            localPatient2 = pservice.Save(patient2);
            //Thread.Sleep(1000);
            masters = pservice.Find(o => o.Identifiers.Any(i => i.Value.Contains("MDM-5")));
            // There should now be one master
            Assert.AreEqual(1, masters.Count());
            Assert.AreEqual(2, masters.First().Relationships.Count(r => r.RelationshipTypeKey == MdmConstants.MasterRecordRelationship));
            Assert.AreEqual(3, masters.First().Identifiers.Count());
            Assert.AreEqual(1, masters.First().Names.Count());
        }

        /// <summary>
        /// Test that when a matching record is updated to be a non-match that the non-matching local record is detached from the master
        /// </summary>
        [Test]
        public void TestMatchUpdatedToNonMatch()
        {
            var pservice = ApplicationServiceContext.Current.GetService<IRepositoryService<Patient>>();
            var patient1 = new Patient()
            {
                DateOfBirth = DateTime.Parse("1985-06-04"),
                Names = new List<EntityName>()
                {
                    new EntityName(NameUseKeys.Legal, "Smith", "Testy")
                },
                Identifiers = new List<EntityIdentifier>()
                {
                    new EntityIdentifier(new AssigningAuthority("TEST-MDM", "TEST-MDM", "1.2.3.4.5.5"), "MDM-6A")
                },
                GenderConceptKey = Guid.Parse("F4E3A6BB-612E-46B2-9F77-FF844D971198")
            };
            var localPatient1 = pservice.Insert(patient1);
            var patient2 = new Patient()
            {
                DateOfBirth = DateTime.Parse("1985-06-04"),
                Names = new List<EntityName>()
                {
                    new EntityName(NameUseKeys.Legal, "Smith", "Testy")
                },
                Identifiers = new List<EntityIdentifier>()
                {
                    new EntityIdentifier(new AssigningAuthority("TEST-MDM", "TEST-MDM", "1.2.3.4.5.6"), "MDM-6B")
                },
                GenderConceptKey = Guid.Parse("F4E3A6BB-612E-46B2-9F77-FF844D971198")
            };
            var localPatient2 = pservice.Insert(patient2);

            // There should be one masters
            //Thread.Sleep(1000);
            var masters = pservice.Find(o => o.Identifiers.Any(i => i.Value.Contains("MDM-6")));
            Assert.AreEqual(1, masters.Count());

            // Now we want to update the second local patient
            // The expected behavior is that there will be two masters with the second detached
            patient2.DateOfBirth = new DateTime(1985, 06, 06);
            localPatient2 = pservice.Save(patient2);
            //Thread.Sleep(1000);
            masters = pservice.Find(o => o.Identifiers.Any(i => i.Value.Contains("MDM-6")));
            Assert.AreEqual(2, masters.Count());

        }

        /// <summary>
        /// Tests that when two local records are matched to a single master and a local is updated, 
        /// that the master remains the only master and the information is reflected
        /// </summary>
        [Test]
        public void UpdateToMatchShouldReflectWithNoNewMaster()
        {
            var pservice = ApplicationServiceContext.Current.GetService<IRepositoryService<Patient>>();
            var patient1 = new Patient()
            {
                DateOfBirth = DateTime.Parse("1985-07-04"),
                Names = new List<EntityName>()
                {
                    new EntityName(NameUseKeys.Legal, "Smith", "Testy")
                },
                Identifiers = new List<EntityIdentifier>()
                {
                    new EntityIdentifier(new AssigningAuthority("TEST-MDM", "TEST-MDM", "1.2.3.4.5.5"), "MDM-7A")
                },
                GenderConceptKey = Guid.Parse("F4E3A6BB-612E-46B2-9F77-FF844D971198")
            };
            var localPatient1 = pservice.Insert(patient1);
            var patient2 = new Patient()
            {
                DateOfBirth = DateTime.Parse("1985-07-04"),
                Names = new List<EntityName>()
                {
                    new EntityName(NameUseKeys.Legal, "Smith", "Testy")
                },
                Identifiers = new List<EntityIdentifier>()
                {
                    new EntityIdentifier(new AssigningAuthority("TEST7", "TEST7", "1.2.3.4.5.7"), "MDM-7B")
                },
                GenderConceptKey = Guid.Parse("F4E3A6BB-612E-46B2-9F77-FF844D971198")
            };
            var localPatient2 = pservice.Insert(patient2);

            // Thread.Sleep(1000);
            var masters = pservice.Find(o => o.Identifiers.Any(i => i.Value.Contains("MDM-7")));
            Assert.AreEqual(1, masters.Count());
            Assert.AreEqual(1, masters.First().Names.Count);
            Assert.AreEqual(0, masters.First().Addresses.Count);

            // Update patient 2 to have a new name and an address
            patient2.Names.Clear();
            patient2.Names.Add(new EntityName(NameUseKeys.OfficialRecord, "SMITH", "JOHN"));
            patient2.Addresses.Add(new EntityAddress(AddressUseKeys.HomeAddress, "123 Main Street West", "Hamilton", "ON", "CA", "L8K5N2"));
            localPatient2 = pservice.Save(patient2);

            // Thread.Sleep(1000);
            masters = pservice.Find(o => o.Identifiers.Any(i => i.Value.Contains("MDM-7")));
            Assert.AreEqual(1, masters.Count());
            Assert.AreEqual(2, masters.First().Names.Count);
            Assert.IsTrue(masters.First().Names.Any(n => n.Component.Any(c => c.Value == "SMITH")));
            Assert.AreEqual(1, masters.First().Addresses.Count);
            Assert.AreEqual(3, masters.First().Identifiers.Count);
            Assert.AreEqual("Male", masters.First().LoadProperty<Concept>("GenderConcept").Mnemonic);
        }

        /// <summary>
        /// Tests that when a master record contains taboo information, it is not disclosed on query.
        /// </summary>
        [Test]
        public void TestTabooInformationNotDisclosedInMaster()
        {
            AuthenticationContext.Current = new AuthenticationContext(AuthenticationContext.SystemPrincipal);
            var pservice = ApplicationServiceContext.Current.GetService<IRepositoryService<Patient>>();
            var patient1 = new Patient()
            {
                DateOfBirth = DateTime.Parse("1985-07-06"),
                Names = new List<EntityName>()
                {
                    new EntityName(NameUseKeys.Legal, "Smith", "Testy")
                },
                Identifiers = new List<EntityIdentifier>()
                {
                    new EntityIdentifier(new AssigningAuthority("TEST-MDM", "TEST-MDM", "1.2.3.4.5.5"), "MDM-8A")
                },
                GenderConceptKey = Guid.Parse("F4E3A6BB-612E-46B2-9F77-FF844D971198")
            };
            var localPatient1 = pservice.Insert(patient1);

            // Here patient 2 has some unique identifier information for an HIV clinic including his HIV alias name
            var patient2 = new Patient()
            {
                DateOfBirth = DateTime.Parse("1985-07-06"),
                Names = new List<EntityName>()
                {
                    new EntityName(NameUseKeys.Anonymous, "John Brenner")
                },
                Identifiers = new List<EntityIdentifier>()
                {
                    new EntityIdentifier(new AssigningAuthority("HIVCLINIC", "HIVCLINIC", "9.9.9.9.9"), "MDM-8B")
                },
                GenderConceptKey = Guid.Parse("F4E3A6BB-612E-46B2-9F77-FF844D971198")
            };
            patient2.AddPolicy(DataPolicyIdentifiers.RestrictedInformation);
            var localPatient2 = pservice.Insert(patient2);

            // Thread.Sleep(1000);
            var masters = pservice.Find(o => o.Identifiers.Any(i => i.Value.Contains("MDM-8")));
            Assert.AreEqual(1, masters.Count());
            // Note that only one identifier from the locals can be in the master synthetic
            Assert.AreEqual(1, masters.First().Names.Count);
            Assert.AreEqual(2, masters.First().Identifiers.Count);
            Assert.IsFalse(masters.First().Identifiers.Any(o => o.Value == "MDM-8B")); // Shoudl not contain the HIV identifier
        }

        /// <summary>
        /// Tests that a master record with Taboo information properly almagamates information from the locals when the current user principal is elevated properly
        /// </summary>
        [Test]
        public void TestShouldShowTabooInformationForAppropriateUser()
        {
            AuthenticationContext.Current = new AuthenticationContext(AuthenticationContext.SystemPrincipal);
            // Create a user and authenticate as that user which has access to taboo information
            var roleService = ApplicationServiceContext.Current.GetService<IRoleProviderService>();
            roleService.CreateRole("RESTRICTED_USERS", AuthenticationContext.SystemPrincipal);
            var userService = ApplicationServiceContext.Current.GetService<IIdentityProviderService>();
            userService.CreateIdentity("RESTRICTED",  "TEST123", AuthenticationContext.SystemPrincipal);
            roleService.AddUsersToRoles(new string[] { "RESTRICTED" }, new string[] { "RESTRICTED_USERS",  "CLINICAL_STAFF" }, AuthenticationContext.SystemPrincipal);
            // Add security policy to the newly created role
            var role = ApplicationServiceContext.Current.GetService<ISecurityRepositoryService>().GetRole("RESTRICTED_USERS");
            ApplicationServiceContext.Current.GetService<IPolicyInformationService>().AddPolicies(role, PolicyGrantType.Grant, AuthenticationContext.SystemPrincipal, DataPolicyIdentifiers.RestrictedInformation);

             // Now we're going insert two patients, one with HIV data
            var pservice = ApplicationServiceContext.Current.GetService<IRepositoryService<Patient>>();
            var patient1 = new Patient()
            {
                DateOfBirth = DateTime.Parse("1990-07-06"),
                Names = new List<EntityName>()
                {
                    new EntityName(NameUseKeys.Legal, "Smith", "Testy")
                },
                Identifiers = new List<EntityIdentifier>()
                {
                    new EntityIdentifier(new AssigningAuthority("TEST-MDM", "TEST-MDM", "1.2.3.4.5.5"), "MDM-9A")
                },
                GenderConceptKey = Guid.Parse("F4E3A6BB-612E-46B2-9F77-FF844D971198")
            };
            var localPatient1 = pservice.Insert(patient1);

            // Here patient 2 has some unique identifier information for an HIV clinic including his HIV alias name
            var patient2 = new Patient()
            {
                DateOfBirth = DateTime.Parse("1990-07-06"),
                Names = new List<EntityName>()
                {
                    new EntityName(NameUseKeys.Anonymous, "John Brenner")
                },
                Identifiers = new List<EntityIdentifier>()
                {
                    new EntityIdentifier(new AssigningAuthority("HIVCLINIC", "HIVCLINIC", "9.9.9.9.9"), "MDM-9B")
                },
                GenderConceptKey = Guid.Parse("F4E3A6BB-612E-46B2-9F77-FF844D971198")
            };
            patient2.AddPolicy(DataPolicyIdentifiers.RestrictedInformation);
            var localPatient2 = pservice.Insert(patient2);

            // Thread.Sleep(1000);
            // When running as SYSTEM - A user which does not have access
            var masters = pservice.Find(o => o.Identifiers.Any(i => i.Value.Contains("MDM-9")));
            Assert.AreEqual(1, masters.Count());
            // Note that only one identifier from the locals can be in the master synthetic
            Assert.AreEqual(1, masters.First().Names.Count);
            Assert.AreEqual(2, masters.First().Identifiers.Count);
            Assert.IsFalse(masters.First().Identifiers.Any(o => o.Value == "MDM-9B")); // Shoudl not contain the HIV identifier
            Assert.AreEqual(1, masters.First().Policies.Count);  // Should not contains any policies

            // When running as our user which has access
            var restrictedUser = userService.Authenticate("RESTRICTED", "TEST123");
            AuthenticationContext.Current = new AuthenticationContext(restrictedUser);
            masters = pservice.Find(o => o.Identifiers.Any(i => i.Value.Contains("MDM-9")));
            Assert.AreEqual(1, masters.Count());
            // Note that two identifiers from the locals should be in the synthetic
            Assert.AreEqual(2, masters.First().Names.Count);
            Assert.AreEqual(3, masters.First().Identifiers.Count);
            Assert.IsTrue(masters.First().Identifiers.Any(o => o.Value == "MDM-9B")); // Shoudl not contain the HIV identifier
            Assert.AreEqual(1, masters.First().Policies.Count);

        }

        /// <summary>
        /// Test - When submitting patients in a bundle each of the individual MDM rules should run
        /// </summary>
        [Test]
        public void TestShouldProcessBundle()
        {
            var patientUnderTest = new Patient()
            {
                DateOfBirth = DateTime.Parse("1983-02-20"),
                Names = new List<EntityName>()
                {
                    new EntityName(NameUseKeys.Legal, "Smith", "Testy")
                },
                Identifiers = new List<EntityIdentifier>()
                {
                    new EntityIdentifier(new AssigningAuthority("TEST-MDM", "TEST-MDM", "1.2.3.4.999"), "MDM-10")
                },
                GenderConceptKey = Guid.Parse("F4E3A6BB-612E-46B2-9F77-FF844D971198")
            };

            try
            {
                ApplicationServiceContext.Current.GetService<IRepositoryService<Bundle>>().Insert(new Bundle()
                {
                    Item = { patientUnderTest }
                });
            }
            catch (Exception e)
            {
                Debug.WriteLine(e.ToString());
                Assert.Fail(e.Message);
            }
            // The creation / matching of a master record may take some time, so we need to wait for the matcher to finish
            //Thread.Sleep(1000);

            // Now attempt to query for the record just created, it should be a synthetic MASTER record
            var masterPatient = ApplicationServiceContext.Current.GetService<IRepositoryService<Patient>>().Find(o => o.Identifiers.Any(i => i.Value == "MDM-10"));
            Assert.AreEqual(1, masterPatient.Count());
            Assert.AreEqual("MDM-10", masterPatient.First().Identifiers.Last().Value);
            Assert.AreEqual("M", masterPatient.First().Tags.First(o => o.TagKey == "$mdm.type").Value);
            Assert.AreEqual("$mdm.processed", masterPatient.First().Tags.First().TagKey);
            Assert.AreEqual(patientUnderTest.Key, masterPatient.First().Relationships.First().SourceEntityKey); // Ensure master is pointed properly

            // Should redirect a retrieve request
            var masterGet = ApplicationServiceContext.Current.GetService<IRepositoryService<Patient>>().Get(masterPatient.First().Key.Value, Guid.Empty);
            Assert.AreEqual(masterPatient.First().Key, masterGet.Key);
            Assert.AreEqual("MDM-10", masterGet.Identifiers.Last().Value);
            Assert.AreEqual("M", masterGet.Tags.First(o => o.TagKey == "$mdm.type").Value);
            Assert.AreEqual("$mdm.processed", masterGet.Tags.First().TagKey);

        }

        /// <summary>
        /// Tests that the MDM layer actually creates a new local when a master is attempted to be updated
        /// </summary>
        /// <remarks>
        /// This test will ensure that the MDM subsystem will actually create a new local record from a different device 
        /// and that the local record is subjected to matching (i.e. the local record does not have the data associated 
        /// with the master record). 
        /// </remarks>
        [Test]
        public void TestShouldCreateLocalRecordWhenAttemptingToCreateMaster()
        {

            var deviceIdentityService = ApplicationServiceContext.Current.GetService<IDeviceIdentityProviderService>();
            var applicationIdentityService = ApplicationServiceContext.Current.GetService<IApplicationIdentityProviderService>();
            var pipService = ApplicationServiceContext.Current.GetService<IPolicyInformationService>();

            if (deviceIdentityService.GetIdentity("DCG_A") == null)
                pipService.AddPolicies(ApplicationServiceContext.Current.GetService<IDataPersistenceService<SecurityDevice>>().Insert(
                    new SecurityDevice()
                    {
                        Name = "DCG_A",
                        DeviceSecret = ApplicationServiceContext.Current.GetService<IPasswordHashingService>().ComputeHash("Test123")
                    }, TransactionMode.Commit, AuthenticationContext.SystemPrincipal), PolicyGrantType.Grant, AuthenticationContext.SystemPrincipal, PermissionPolicyIdentifiers.Login);

            if (deviceIdentityService.GetIdentity("DCG_B") == null)
                pipService.AddPolicies(ApplicationServiceContext.Current.GetService<IDataPersistenceService<SecurityDevice>>().Insert(
                    new SecurityDevice()
                    {
                        Name = "DCG_B",
                        DeviceSecret = ApplicationServiceContext.Current.GetService<IPasswordHashingService>().ComputeHash("Test123")
                    }, TransactionMode.Commit, AuthenticationContext.SystemPrincipal), PolicyGrantType.Grant, AuthenticationContext.SystemPrincipal, PermissionPolicyIdentifiers.Login);

            if (applicationIdentityService.GetIdentity("DCG") == null)
                pipService.AddPolicies(ApplicationServiceContext.Current.GetService<IDataPersistenceService<SecurityApplication>>().Insert(
                    new SecurityApplication()
                    {
                        Name = "DCG",
                        ApplicationSecret = ApplicationServiceContext.Current.GetService<IPasswordHashingService>().ComputeHash("Test123")
                    }, TransactionMode.Commit, AuthenticationContext.SystemPrincipal), PolicyGrantType.Grant, AuthenticationContext.SystemPrincipal, PermissionPolicyIdentifiers.Login);

            // First, establish identity as DCG_A
            var dcgIdentity = applicationIdentityService.Authenticate("DCG", "Test123");
            var deviceAIdentity = deviceIdentityService.Authenticate("DCG_A", "Test123");
            var deviceBIdentity = deviceIdentityService.Authenticate("DCG_B", "Test123");

            // Principal A and Principal B
            var principalA = new SanteDBClaimsPrincipal(new IClaimsIdentity[] { deviceAIdentity.Identity as IClaimsIdentity , dcgIdentity.Identity as IClaimsIdentity });
            var principalB = new SanteDBClaimsPrincipal(new IClaimsIdentity[] { deviceBIdentity.Identity as IClaimsIdentity , dcgIdentity.Identity as IClaimsIdentity });

            // Now authenticate as Device A and create a new master
            var patientUnderTest = new Patient()
            {
                DateOfBirth = DateTime.Parse("1984-03-20"),
                Names = new List<EntityName>()
                {
                    new EntityName(NameUseKeys.Legal, "Smith", "Device")
                },
                Identifiers = new List<EntityIdentifier>()
                {
                    new EntityIdentifier(new AssigningAuthority("TEST-MDM", "TEST-MDM", "1.2.3.4.999"), "MDM-11")
                },
                GenderConceptKey = Guid.Parse("F4E3A6BB-612E-46B2-9F77-FF844D971198")
            };

            // Should create a local patient
            var patientService = ApplicationServiceContext.Current.GetService<IRepositoryService<Patient>>();
            AuthenticationContext.Current = new AuthenticationContext(principalA);
            var deviceAPatient = patientService.Insert(patientUnderTest);

            // Now authenticate as device B - Simulates what a local would do to the record when performing an update by a field in the master
            AuthenticationContext.Current = new AuthenticationContext(principalB);
            var nhid = NationalHealthIdRule.LastGeneratedNhid.ToString();
            var masterPatient = patientService.Find(o => o.Identifiers.Any(i => i.Authority.DomainName == "NHID" && i.Value == nhid)).SingleOrDefault();
            Assert.AreEqual(1, masterPatient.Relationships.Where(o => o.RelationshipTypeKey == MdmConstants.MasterRecordRelationship).Count());
            Assert.IsNotNull(masterPatient);
            Assert.AreEqual("M", masterPatient.Tags.FirstOrDefault(o => o.TagKey == "$mdm.type").Value);

            // Attempt to update the patient as device B -> Should result in a new record being created with the master
            masterPatient.Addresses.Add(new EntityAddress(AddressUseKeys.Public, "123 Main Street", "Hamilton", "ON", "CA", null));
            patientService.Save(masterPatient);

            // Now authenticate as SYSTEM and ensure that proper master was created
            AuthenticationContext.Current = new AuthenticationContext(AuthenticationContext.SystemPrincipal);
            masterPatient = patientService.Find(o => o.Identifiers.Any(i => i.Authority.DomainName == "NHID" && i.Value == nhid)).SingleOrDefault();
            Assert.AreEqual(2, masterPatient.Relationships.Where(o => o.RelationshipTypeKey == MdmConstants.MasterRecordRelationship).Count());
            Assert.AreEqual(1, masterPatient.Relationships.First().LoadProperty<Entity>(nameof(EntityRelationship.SourceEntity)).Identifiers.Count);
            Assert.AreEqual(2, masterPatient.Relationships.Last().LoadProperty<Entity>(nameof(EntityRelationship.SourceEntity)).Identifiers.Count);
        }
        
    }
}
