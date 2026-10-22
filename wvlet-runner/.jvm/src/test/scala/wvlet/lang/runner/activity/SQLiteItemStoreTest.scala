package wvlet.lang.runner.activity

import wvlet.uni.test.UniTest
import wvlet.uni.util.ULID

import java.nio.file.Files

class SQLiteItemStoreTest extends UniTest:

  private def newStore(): SQLiteItemStore =
    val dir = Files.createTempDirectory("wv-item-store-test")
    SQLiteItemStore(dir.resolve("items.db"))

  test("save, get, and list items") {
    val store = newStore()
    val runId = "run-1"

    val item1 = ItemRecord(
      itemId = "item-1",
      runId = runId,
      stageName = "stage-a",
      itemKey = "key-1",
      state = ItemState.Queued.stateName,
      attemptId = "attempt-1",
      attempts = 0
    )
    val item2 = ItemRecord(
      itemId = "item-2",
      runId = runId,
      stageName = "stage-a",
      itemKey = "key-2",
      state = ItemState.Running.stateName,
      attemptId = "attempt-2",
      attempts = 1,
      startedAtMillis = Some(1000L),
      leaseExpiresAtMillis = Some(61_000L)
    )

    store.saveItem(item1)
    store.saveItem(item2)

    store.getItem("item-1").get.state shouldBe "queued"
    store.getItem("item-2").get.state shouldBe "running"
    store.getItem("item-2").get.leaseExpiresAtMillis shouldBe Some(61_000L)

    val items = store.listItems(runId, "stage-a")
    items.size shouldBe 2
    items.head.itemKey shouldBe "key-1"

    store.getItem("nonexistent") shouldBe None
    store.close()
  }

  test("reportResult accepts matching attemptId") {
    val store = newStore()
    val item  = ItemRecord(
      itemId = "item-report",
      runId = "run-1",
      stageName = "stage-a",
      itemKey = "key-1",
      state = ItemState.Running.stateName,
      attemptId = "attempt-a",
      attempts = 1,
      startedAtMillis = Some(System.currentTimeMillis()),
      leaseExpiresAtMillis = Some(System.currentTimeMillis() + 60_000)
    )
    store.saveItem(item)

    val accepted = store.reportResult(
      "item-report",
      "attempt-a",
      ItemState.Succeeded.stateName,
      Some("""{"result":"ok"}"""),
      None,
      System.currentTimeMillis()
    )
    accepted shouldBe true

    val updated = store.getItem("item-report").get
    updated.state shouldBe "succeeded"
    updated.output shouldBe Some("""{"result":"ok"}""")
    updated.leaseExpiresAtMillis shouldBe None
    store.close()
  }

  test("reportResult rejects stale attemptId") {
    val store = newStore()
    val item  = ItemRecord(
      itemId = "item-stale",
      runId = "run-1",
      stageName = "stage-a",
      itemKey = "key-1",
      state = ItemState.Running.stateName,
      attemptId = "current-attempt",
      attempts = 1,
      startedAtMillis = Some(System.currentTimeMillis()),
      leaseExpiresAtMillis = Some(System.currentTimeMillis() + 60_000)
    )
    store.saveItem(item)

    val rejected = store.reportResult(
      "item-stale",
      "wrong-attempt",
      ItemState.Succeeded.stateName,
      Some("{}"),
      None,
      System.currentTimeMillis()
    )
    rejected shouldBe false

    // State should not change
    store.getItem("item-stale").get.state shouldBe "running"
    store.close()
  }

  test("refreshItemLease updates lease for matching attempt") {
    val store = newStore()
    val item  = ItemRecord(
      itemId = "item-lease",
      runId = "run-1",
      stageName = "stage-a",
      itemKey = "key-1",
      state = ItemState.Running.stateName,
      attemptId = "attempt-lease",
      attempts = 1,
      startedAtMillis = Some(1000L),
      leaseExpiresAtMillis = Some(2000L)
    )
    store.saveItem(item)

    store.refreshItemLease("item-lease", "attempt-lease", 99_999L)
    store.getItem("item-lease").get.leaseExpiresAtMillis shouldBe Some(99_999L)
    store.close()
  }

  test("deleteItems removes all items for a run") {
    val store = newStore()
    store.saveItem(
      ItemRecord(
        itemId = "item-del-1",
        runId = "run-del",
        stageName = "stage-a",
        itemKey = "k1",
        state = "queued",
        attemptId = "a1",
        attempts = 0
      )
    )
    store.saveItem(
      ItemRecord(
        itemId = "item-del-2",
        runId = "run-del",
        stageName = "stage-a",
        itemKey = "k2",
        state = "queued",
        attemptId = "a2",
        attempts = 0
      )
    )
    store.saveItem(
      ItemRecord(
        itemId = "item-other",
        runId = "run-other",
        stageName = "stage-a",
        itemKey = "k3",
        state = "queued",
        attemptId = "a3",
        attempts = 0
      )
    )

    store.deleteItems("run-del")
    store.listItems("run-del", "stage-a").size shouldBe 0
    store.listItems("run-other", "stage-a").size shouldBe 1
    store.close()
  }

  test("upsert overwrites existing item") {
    val store = newStore()
    val item  = ItemRecord(
      itemId = "item-upsert",
      runId = "run-1",
      stageName = "stage-a",
      itemKey = "key-1",
      state = ItemState.Queued.stateName,
      attemptId = "attempt-1",
      attempts = 0
    )
    store.saveItem(item)

    val updated = item.copy(
      state = ItemState.Running.stateName,
      attemptId = "attempt-2",
      attempts = 1,
      startedAtMillis = Some(5000L)
    )
    store.saveItem(updated)

    val fetched = store.getItem("item-upsert").get
    fetched.state shouldBe "running"
    fetched.attemptId shouldBe "attempt-2"
    fetched.attempts shouldBe 1
    fetched.startedAtMillis shouldBe Some(5000L)
    store.close()
  }

  test("ItemRecord.itemId is deterministic") {
    val id1 = ItemRecord.itemId("run-x", "stage-y", "key-z")
    val id2 = ItemRecord.itemId("run-x", "stage-y", "key-z")
    id1 shouldBe id2

    val id3 = ItemRecord.itemId("run-x", "stage-y", "key-w")
    (id3 == id1) shouldBe false
  }

end SQLiteItemStoreTest
