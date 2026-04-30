package wvlet.lang.server

import wvlet.lang.api.WvletLangException
import wvlet.lang.api.v1.frontend.FileApi
import wvlet.lang.compiler.WorkEnv
import wvlet.lang.test.WvletDITest

class FileApiImplTest extends WvletDITest:

  initDesign:
    _.bindImpl[FileApi, FileApiImpl].bindInstance[WorkEnv](WorkEnv("spec/basic"))

  test("list files") {
    val api = dep[FileApi]
    val lst = api.listFiles(FileApi.FileRequest(""))
    lst shouldNotBe empty
    lst.filter(_.isFile).forall(_.name.endsWith(".wv")) shouldBe true

    lst.filter(_.isDirectory) shouldNotBe empty
  }

  test("get empty path") {
    val api = dep[FileApi]
    val lst = api.getPath(FileApi.FileRequest(""))
    lst shouldBe empty
  }

  test("get single path") {
    val api = dep[FileApi]
    val lst = api.getPath(FileApi.FileRequest("update"))
    lst shouldNotBe empty
    lst.size shouldBe 1
    lst.head.path shouldBe "update"
  }

  test("get multiple paths") {
    val api = dep[FileApi]
    val lst = api.getPath(FileApi.FileRequest("update/append.wv"))
    lst shouldNotBe empty
    lst.size shouldBe 2
    lst.map(_.path) shouldBe Seq("update", "update/append.wv")
    lst.head.isFile shouldBe false
    lst.last.isFile shouldBe true
  }

  test("reject invalid path") {
    val api = dep[FileApi]
    val ex  = intercept[WvletLangException] {
      api.listFiles(FileApi.FileRequest("../"))
    }
    ex.statusCode.isUserError shouldBe true
  }

end FileApiImplTest
