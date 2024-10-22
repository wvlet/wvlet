package wvlet.lang.server

import wvlet.airspec.AirSpec
import wvlet.lang.api.WvletLangException
import wvlet.lang.api.v1.frontend.FileApi
import wvlet.lang.compiler.WorkEnv

class FileApiImplTest extends AirSpec:

  initDesign:
    _.bindImpl[FileApi, FileApiImpl].bindInstance[WorkEnv](WorkEnv("spec/basic"))

  test("list files") { (api: FileApi) =>
    val f = api.listFiles(FileApi.FileRequest(""))
    debug(f.children.mkString("\n"))
    f.children shouldNotBe empty
    f.children.filter(_.isFile).forall(_.name.endsWith(".wv")) shouldBe true

    f.children.filter(_.isDirectory) shouldNotBe empty
  }

  test("reject invalid path") { (api: FileApi) =>
    val ex = intercept[WvletLangException] {
      api.listFiles(FileApi.FileRequest("../"))
    }
    ex.statusCode.isUserError shouldBe true
  }
