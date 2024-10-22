package wvlet.lang.server

import wvlet.airspec.AirSpec
import wvlet.lang.api.WvletLangException
import wvlet.lang.api.v1.frontend.FileApi
import wvlet.lang.compiler.WorkEnv

class FileApiImplTest extends AirSpec:

  initDesign:
    _.bindImpl[FileApi, FileApiImpl].bindInstance[WorkEnv](WorkEnv("spec/basic"))

  test("list files") { (api: FileApi) =>
    val list = api.fileList(FileApi.FileListRequest(""))
    debug(list.files.mkString("\n"))
    list.files shouldNotBe empty
    list.files.filter(_.isFile).forall(_.name.endsWith(".wv")) shouldBe true

    list.files.filter(_.isDirectory) shouldNotBe empty
  }

  test("reject invalid path") { (api: FileApi) =>
    val ex = intercept[WvletLangException] {
      api.fileList(FileApi.FileListRequest("../"))
    }
    ex.statusCode.isUserError shouldBe true
  }
