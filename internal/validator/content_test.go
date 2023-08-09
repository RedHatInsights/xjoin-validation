package validator_test

import (
	"github.com/DATA-DOG/go-sqlmock"
	"github.com/RedHatInsights/xjoin-validation/internal/test"
	. "github.com/RedHatInsights/xjoin-validation/internal/validator"
	"github.com/jarcoal/httpmock"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Content validation", func() {
	var validator Validator
	var dbMock sqlmock.Sqlmock

	BeforeEach(func() {
		testEnv := test.BeforeEach()
		validator = testEnv.Validator
		dbMock = testEnv.DBMock
	})

	AfterEach(func() {
		httpmock.DeactivateAndReset()
	})

	Context("should be valid", func() {
		It("when database and elasticsearch content is the same", func() {
			validator.SetDBIDs([]string{"1234"})

			dbMock.
				ExpectQuery(
					`SELECT id,account,display_name,created_on,modified_on,facts,tags,canonical_facts,system_profile_facts,ansible_host,stale_timestamp,reporter,per_reporter_staleness,org_id FROM hosts WHERE ID IN ('1234') ORDER BY id`).
				WillReturnRows(sqlmock.NewRows([]string{
					"id",
					"account",
					"display_name",
					"created_on",
					"modified_on",
					"facts",
					"tags",
					"canonical_facts",
					"system_profile_facts",
					"ansible_host",
					"stale_timestamp",
					"reporter",
					"per_reporter_staleness",
					"org_id",
				}).AddRow(
					"1234",
					nil,
					"a96dac.foo.redhat.com",
					"2023-01-04T14:40:54.825995Z",
					"2023-01-04T14:40:54.826002Z",
					"{}",
					`{"Sat": {"prod": []},"NS1": {"key3": ["val3"]},"SPECIAL": {"key": ["val"]},"NS3": {"key3": ["val3"]}}`,
					`{"bios_uuid": "fa067396-2449-4f16-83a3-b8fc32e040a6"}`,
					`{"insights_egg_version": "120.0.1","rhc_client_id": "044e36dc-4e2b-4e69-8948-9c65a7bf4976","owner_id": "1b36b20f-7fa0-4454-a6d2-008294e06378","yum_repos": [{"gpgcheck": true,"name": "repo1","base_url": "http://rpms.redhat.com","enabled": true}],"os_release": "Red Hat EL 7.0.1","installed_products": [{"name": "eap","id": "123","status": "UP"},{"name": "jbws","id": "321","status": "DOWN"}],"infrastructure_type": "jingleheimer junction cpu","cores_per_socket": 4,"installed_services": ["ndb","krb5"],"bios_vendor": "Turd Ferguson","number_of_cpus": 1,"insights_client_version": "12.0.12","kernel_modules": ["i915","e1000e"],"cpu_model": "Intel(R) Xeon(R) CPU E5-2690 0 @ 2.90GHz","subscription_status": "valid","system_memory_bytes": 1024,"is_marketplace": false,"operating_system": {"major": 8,"minor": 1,"name": "RHEL"},"selinux_current_mode": "enforcing","katello_agent_running": false,"last_boot_time": "2020-02-13T12:08:55Z","enabled_services": ["ndb","krb5"],"number_of_sockets": 2,"running_processes": ["vim","gcc","python"],"bios_release_date": "10/31/2013","disk_devices": [{"mount_point": "/home","options": {"uid": "0","ro": true},"label": "home drive","type": "ext3","device": "/dev/sdb1"}],"selinux_config_file": "enforcing","bios_version": "1.0.0uhoh","os_kernel_version": "3.10.0","captured_date": "2020-02-13T12:16:00Z","cpu_flags": ["flag1","flag2"],"network_interfaces": [{"ipv6_addresses": ["2001:0db8:85a3:0000:0000:8a2e:0370:7334"],"mac_address": "aa:bb:cc:dd:ee:ff","name": "eth0","ipv4_addresses": ["10.10.10.1"],"state": "UP","type": "loopback","mtu": 1500}],"rhc_config_state": "044e36dc-4e2b-4e69-8948-9c65a7bf4976","subscription_auto_attach": "yes","arch": "x86-64","satellite_managed": false,"infrastructure_vendor": "dell"}`,
					nil,
					"2023-01-05T14:40:54.787157Z",
					"puptoo",
					`{"puptoo": {"check_in_succeeded": true,"stale_timestamp": "2023-01-05T14:40:54.787157+00:00","last_check_in": "2023-01-04T14:40:54.817771+00:00"}}`,
					"test"))

			httpmock.RegisterResponder(
				"GET",
				"http://mock-es:9200/mockindex/_search?size=1&sort=_id",
				httpmock.NewStringResponder(200, test.LoadTestDataFile("elasticsearch/content/one.hit.response")))

			result, err := validator.ValidateContent()
			Expect(err).ToNot(HaveOccurred())

			Expect(result).To(Equal(ValidateContentResult{
				MismatchCount:         0,
				MismatchRatio:         0,
				ContentIsValid:        true,
				MismatchedRecords:     nil,
				MismatchedIDs:         []string{},
				TotalRecordsValidated: 1,
			}))

			info := httpmock.GetCallCountInfo()
			count := info["GET http://mock-es:9200/mockindex/_search?size=1&sort=_id"]
			Expect(count).To(Equal(1))
		})
	})

	Context("should be invalid", func() {
		It("when database and elasticsearch content are not the same", func() {
			validator.SetDBIDs([]string{"1234"})

			//the query is done twice to account for lag
			//sqlmock doesn't support matching a query multiple times
			//https://github.com/DATA-DOG/go-sqlmock/pull/257
			for i := 1; i <= 2; i++ {
				dbMock.
					ExpectQuery(
						`SELECT id,account,display_name,created_on,modified_on,facts,tags,canonical_facts,system_profile_facts,ansible_host,stale_timestamp,reporter,per_reporter_staleness,org_id FROM hosts WHERE ID IN ('1234') ORDER BY id`).
					WillReturnRows(sqlmock.NewRows([]string{
						"id",
						"account",
						"display_name",
						"created_on",
						"modified_on",
						"facts",
						"tags",
						"canonical_facts",
						"system_profile_facts",
						"ansible_host",
						"stale_timestamp",
						"reporter",
						"per_reporter_staleness",
						"org_id",
					}).AddRow(
						"1234",
						nil,
						"DIFFERENT DISPLAY NAME",
						"2023-01-04T14:40:54.825995Z",
						"2023-01-04T14:40:54.826002Z",
						"{}",
						`{"Sat": {"prod": []},"NS1": {"key3": ["val3"]},"SPECIAL": {"key": ["val"]},"NS3": {"key3": ["val3"]}}`,
						`{"bios_uuid": "fa067396-2449-4f16-83a3-b8fc32e040a6"}`,
						`{"insights_egg_version": "120.0.1","rhc_client_id": "044e36dc-4e2b-4e69-8948-9c65a7bf4976","owner_id": "1b36b20f-7fa0-4454-a6d2-008294e06378","yum_repos": [{"gpgcheck": true,"name": "repo1","base_url": "http://rpms.redhat.com","enabled": true}],"os_release": "Red Hat EL 7.0.1","installed_products": [{"name": "eap","id": "123","status": "UP"},{"name": "jbws","id": "321","status": "DOWN"}],"infrastructure_type": "jingleheimer junction cpu","cores_per_socket": 4,"installed_services": ["ndb","krb5"],"bios_vendor": "Turd Ferguson","number_of_cpus": 1,"insights_client_version": "12.0.12","kernel_modules": ["i915","e1000e"],"cpu_model": "Intel(R) Xeon(R) CPU E5-2690 0 @ 2.90GHz","subscription_status": "valid","system_memory_bytes": 1024,"is_marketplace": false,"operating_system": {"major": 8,"minor": 1,"name": "RHEL"},"selinux_current_mode": "enforcing","katello_agent_running": false,"last_boot_time": "2020-02-13T12:08:55Z","enabled_services": ["ndb","krb5"],"number_of_sockets": 2,"running_processes": ["vim","gcc","python"],"bios_release_date": "10/31/2013","disk_devices": [{"mount_point": "/home","options": {"uid": "0","ro": true},"label": "home drive","type": "ext3","device": "/dev/sdb1"}],"selinux_config_file": "enforcing","bios_version": "1.0.0uhoh","os_kernel_version": "3.10.0","captured_date": "2020-02-13T12:16:00Z","cpu_flags": ["flag1","flag2"],"network_interfaces": [{"ipv6_addresses": ["2001:0db8:85a3:0000:0000:8a2e:0370:7334"],"mac_address": "aa:bb:cc:dd:ee:ff","name": "eth0","ipv4_addresses": ["10.10.10.1"],"state": "UP","type": "loopback","mtu": 1500}],"rhc_config_state": "044e36dc-4e2b-4e69-8948-9c65a7bf4976","subscription_auto_attach": "yes","arch": "x86-64","satellite_managed": false,"infrastructure_vendor": "dell"}`,
						nil,
						"2023-01-05T14:40:54.787157Z",
						"puptoo",
						`{"puptoo": {"check_in_succeeded": true,"stale_timestamp": "2023-01-05T14:40:54.787157+00:00","last_check_in": "2023-01-04T14:40:54.817771+00:00"}}`,
						"test"))
			}

			httpmock.RegisterResponder(
				"GET",
				"http://mock-es:9200/mockindex/_search?size=1&sort=_id",
				httpmock.NewStringResponder(200, test.LoadTestDataFile("elasticsearch/content/one.hit.response")))

			result, err := validator.ValidateContent()
			Expect(err).ToNot(HaveOccurred())

			Expect(result.MismatchCount).To(Equal(1))
			Expect(result.MismatchRatio).To(Equal(float64(1)))
			Expect(result.ContentIsValid).To(Equal(false))
			Expect(result.MismatchedIDs).To(ContainElements([]string{"1234"}))
			Expect(result.TotalRecordsValidated).To(Equal(1))
			Expect(result.MismatchedRecords).To(HaveLen(1))
			Expect(result.MismatchedRecords["1234"].DBRecord).To(Not(BeEmpty()))
			Expect(result.MismatchedRecords["1234"].ESDocument).To(Not(BeEmpty()))
			Expect(result.MismatchedRecords["1234"].Diffs).To(ContainElements([]string{"slice[0].map[host].map[display_name]: DIFFERENT DISPLAY NAME != a96dac.foo.redhat.com"}))

			info := httpmock.GetCallCountInfo()
			count := info["GET http://mock-es:9200/mockindex/_search?size=1&sort=_id"]
			Expect(count).To(Equal(2))
		})
	})

	It("when multiple record contents mismatch", func() {

		validator.SetDBIDs([]string{"1234", "5678"})

		//the query is done twice to account for lag
		//sqlmock doesn't support matching a query multiple times
		//https://github.com/DATA-DOG/go-sqlmock/pull/257
		for i := 1; i <= 2; i++ {
			dbMock.
				ExpectQuery(
					`SELECT id,account,display_name,created_on,modified_on,facts,tags,canonical_facts,system_profile_facts,ansible_host,stale_timestamp,reporter,per_reporter_staleness,org_id FROM hosts WHERE ID IN ('1234','5678') ORDER BY id`).
				WillReturnRows(sqlmock.NewRows([]string{
					"id",
					"account",
					"display_name",
					"created_on",
					"modified_on",
					"facts",
					"tags",
					"canonical_facts",
					"system_profile_facts",
					"ansible_host",
					"stale_timestamp",
					"reporter",
					"per_reporter_staleness",
					"org_id",
				}).AddRow(
					"1234",
					nil,
					"DIFFERENT DISPLAY NAME",
					"2023-01-04T14:40:54.825995Z",
					"2023-01-04T14:40:54.826002Z",
					"{}",
					`{"Sat": {"prod": []},"NS1": {"key3": ["val3"]},"SPECIAL": {"key": ["val"]},"NS3": {"key3": ["val3"]}}`,
					`{"bios_uuid": "fa067396-2449-4f16-83a3-b8fc32e040a6"}`,
					`{"insights_egg_version": "120.0.1","rhc_client_id": "044e36dc-4e2b-4e69-8948-9c65a7bf4976","owner_id": "1b36b20f-7fa0-4454-a6d2-008294e06378","yum_repos": [{"gpgcheck": true,"name": "repo1","base_url": "http://rpms.redhat.com","enabled": true}],"os_release": "Red Hat EL 7.0.1","installed_products": [{"name": "eap","id": "123","status": "UP"},{"name": "jbws","id": "321","status": "DOWN"}],"infrastructure_type": "jingleheimer junction cpu","cores_per_socket": 4,"installed_services": ["ndb","krb5"],"bios_vendor": "Turd Ferguson","number_of_cpus": 1,"insights_client_version": "12.0.12","kernel_modules": ["i915","e1000e"],"cpu_model": "Intel(R) Xeon(R) CPU E5-2690 0 @ 2.90GHz","subscription_status": "valid","system_memory_bytes": 1024,"is_marketplace": false,"operating_system": {"major": 8,"minor": 1,"name": "RHEL"},"selinux_current_mode": "enforcing","katello_agent_running": false,"last_boot_time": "2020-02-13T12:08:55Z","enabled_services": ["ndb","krb5"],"number_of_sockets": 2,"running_processes": ["vim","gcc","python"],"bios_release_date": "10/31/2013","disk_devices": [{"mount_point": "/home","options": {"uid": "0","ro": true},"label": "home drive","type": "ext3","device": "/dev/sdb1"}],"selinux_config_file": "enforcing","bios_version": "1.0.0uhoh","os_kernel_version": "3.10.0","captured_date": "2020-02-13T12:16:00Z","cpu_flags": ["flag1","flag2"],"network_interfaces": [{"ipv6_addresses": ["2001:0db8:85a3:0000:0000:8a2e:0370:7334"],"mac_address": "aa:bb:cc:dd:ee:ff","name": "eth0","ipv4_addresses": ["10.10.10.1"],"state": "UP","type": "loopback","mtu": 1500}],"rhc_config_state": "044e36dc-4e2b-4e69-8948-9c65a7bf4976","subscription_auto_attach": "yes","arch": "x86-64","satellite_managed": false,"infrastructure_vendor": "dell"}`,
					nil,
					"2023-01-05T14:40:54.787157Z",
					"puptoo",
					`{"puptoo": {"check_in_succeeded": true,"stale_timestamp": "2023-01-05T14:40:54.787157+00:00","last_check_in": "2023-01-04T14:40:54.817771+00:00"}}`,
					"test",
				).AddRow(
					"5678",
					nil,
					"second host",
					"2023-01-04T14:40:54.825995Z",
					"2023-01-04T14:40:54.826002Z",
					"{}",
					`{"Sat": {"prod": []},"NS1": {"key3": ["val3"]},"SPECIAL": {"key": ["val"]},"NS3": {"key3": ["val3"]}}`,
					`{"bios_uuid": "fa067396-2449-4f16-83a3-b8fc32e040a6"}`,
					`{}`, //different system_profile_facts
					nil,
					"2023-01-05T14:40:54.787157Z",
					"puptoo",
					`{"puptoo": {"check_in_succeeded": true,"stale_timestamp": "2023-01-05T14:40:54.787157+00:00","last_check_in": "2023-01-04T14:40:54.817771+00:00"}}`,
					"test",
				))
		}

		httpmock.Reset()
		responder, err := httpmock.NewJsonResponder(200, httpmock.File(test.GetRootDir()+"/test/elasticsearch/content/two.hit.response.json"))
		Expect(err).ToNot(HaveOccurred())
		httpmock.RegisterResponder(
			"GET",
			"http://mock-es:9200/mockindex/_search?size=2&sort=_id",
			responder)

		result, err := validator.ValidateContent()
		Expect(err).ToNot(HaveOccurred())

		Expect(result.MismatchCount).To(Equal(2))
		Expect(result.MismatchRatio).To(Equal(float64(1)))
		Expect(result.ContentIsValid).To(Equal(false))
		Expect(result.MismatchedIDs).To(ContainElements([]string{"1234", "5678"}))
		Expect(result.TotalRecordsValidated).To(Equal(2))
		Expect(result.MismatchedRecords).To(HaveLen(2))
		Expect(result.MismatchedRecords["1234"].DBRecord).To(Not(BeEmpty()))
		Expect(result.MismatchedRecords["1234"].ESDocument).To(Not(BeEmpty()))
		Expect(result.MismatchedRecords["1234"].Diffs).To(ContainElements([]string{"slice[0].map[host].map[display_name]: DIFFERENT DISPLAY NAME != a96dac.foo.redhat.com"}))
		Expect(result.MismatchedRecords["5678"].Diffs).To(ContainElements([]string{
			"slice[1].map[host].map[system_profile_facts].map[cores_per_socket]: <does not have key> != 4",
			"slice[1].map[host].map[system_profile_facts].map[last_boot_time]: <does not have key> != 2020-02-13T12:08:55Z",
			"slice[1].map[host].map[system_profile_facts].map[captured_date]: <does not have key> != 2020-02-13T12:16:00Z",
			"slice[1].map[host].map[system_profile_facts].map[infrastructure_vendor]: <does not have key> != dell",
			"slice[1].map[host].map[system_profile_facts].map[infrastructure_type]: <does not have key> != jingleheimer junction cpu",
			"slice[1].map[host].map[system_profile_facts].map[number_of_cpus]: <does not have key> != 1",
			"slice[1].map[host].map[system_profile_facts].map[is_marketplace]: <does not have key> != false",
			"slice[1].map[host].map[system_profile_facts].map[owner_id]: <does not have key> != 1b36b20f-7fa0-4454-a6d2-008294e06378",
			"slice[1].map[host].map[system_profile_facts].map[os_release]: <does not have key> != Red Hat EL 7.0.1",
			"slice[1].map[host].map[system_profile_facts].map[subscription_auto_attach]: <does not have key> != yes",
			"slice[1].map[host].map[system_profile_facts].map[network_interfaces]: <does not have key> != [map[ipv4_addresses:[10.10.10.1] ipv6_addresses:[2001:0db8:85a3:0000:0000:8a2e:0370:7334] mac_address:aa:bb:cc:dd:ee:ff mtu:1500 name:eth0 state:UP type:loopback]]",
			"slice[1].map[host].map[system_profile_facts].map[installed_products]: <does not have key> != [map[id:123 name:eap status:UP] map[id:321 name:jbws status:DOWN]]",
			"slice[1].map[host].map[system_profile_facts].map[number_of_sockets]: <does not have key> != 2",
			"slice[1].map[host].map[system_profile_facts].map[os_kernel_version]: <does not have key> != 3.10.0",
			"slice[1].map[host].map[system_profile_facts].map[arch]: <does not have key> != x86-64",
			"slice[1].map[host].map[system_profile_facts].map[satellite_managed]: <does not have key> != false",
			"slice[1].map[host].map[system_profile_facts].map[insights_client_version]: <does not have key> != 12.0.12",
			"slice[1].map[host].map[system_profile_facts].map[operating_system]: <does not have key> != map[major:8 minor:1 name:RHEL]",
			"slice[1].map[host].map[system_profile_facts].map[running_processes]: <does not have key> != [vim gcc python]",
			"slice[1].map[host].map[system_profile_facts].map[bios_release_date]: <does not have key> != 10/31/2013",
			"slice[1].map[host].map[system_profile_facts].map[disk_devices]: <does not have key> != [map[device:/dev/sdb1 label:home drive mount_point:/home options:map[ro:true uid:0] type:ext3]]",
			"slice[1].map[host].map[system_profile_facts].map[rhc_config_state]: <does not have key> != 044e36dc-4e2b-4e69-8948-9c65a7bf4976",
			"slice[1].map[host].map[system_profile_facts].map[yum_repos]: <does not have key> != [map[base_url:http://rpms.redhat.com enabled:true gpgcheck:true name:repo1]]",
			"slice[1].map[host].map[system_profile_facts].map[cpu_model]: <does not have key> != Intel(R) Xeon(R) CPU E5-2690 0 @ 2.90GHz",
			"slice[1].map[host].map[system_profile_facts].map[selinux_current_mode]: <does not have key> != enforcing",
			"slice[1].map[host].map[system_profile_facts].map[subscription_status]: <does not have key> != valid",
			"slice[1].map[host].map[system_profile_facts].map[enabled_services]: <does not have key> != [ndb krb5]",
			"slice[1].map[host].map[system_profile_facts].map[cpu_flags]: <does not have key> != [flag1 flag2]",
			"slice[1].map[host].map[system_profile_facts].map[rhc_client_id]: <does not have key> != 044e36dc-4e2b-4e69-8948-9c65a7bf4976",
			"slice[1].map[host].map[system_profile_facts].map[installed_services]: <does not have key> != [ndb krb5]",
			"slice[1].map[host].map[system_profile_facts].map[kernel_modules]: <does not have key> != [i915 e1000e]",
			"slice[1].map[host].map[system_profile_facts].map[katello_agent_running]: <does not have key> != false",
			"slice[1].map[host].map[system_profile_facts].map[selinux_config_file]: <does not have key> != enforcing",
			"slice[1].map[host].map[system_profile_facts].map[bios_version]: <does not have key> != 1.0.0uhoh",
			"slice[1].map[host].map[system_profile_facts].map[insights_egg_version]: <does not have key> != 120.0.1",
			"slice[1].map[host].map[system_profile_facts].map[bios_vendor]: <does not have key> != Turd Ferguson",
			"slice[1].map[host].map[system_profile_facts].map[system_memory_bytes]: <does not have key> != 1024",
		}))
		Expect(result.MismatchedRecords["5678"].DBRecord).To(Not(BeEmpty()))
		Expect(result.MismatchedRecords["5678"].ESDocument).To(Not(BeEmpty()))

		info := httpmock.GetCallCountInfo()
		count := info["GET http://mock-es:9200/mockindex/_search?size=2&sort=_id"]
		Expect(count).To(Equal(2))
	})
})
