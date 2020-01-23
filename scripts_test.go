package curlyq

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Scripts", func() {
	Describe("loadLua", func() {
		It("Loads all scripts successfully", func() {
			scripts := []string{
				"/lua/ack_job.lua",
				"/lua/enqueue_scheduled_jobs.lua",
				"/lua/get_jobs.lua",
				"/lua/reenqueue_active_jobs.lua",
				"/lua/reenqueue_orphaned_jobs.lua",
				"/lua/register_consumer.lua",
				"/lua/retry_job.lua",
				"/lua/schedule_job.lua",
			}
			for _, script := range scripts {
				Expect(func() {
					loadLua(script)
				}).NotTo(Panic())
			}
		})

		It("Panics when it tries to load a non-extant script", func() {
			Expect(func() {
				loadLua("/lua/missing_script.lua")
			}).To(Panic())
		})
	})
})
