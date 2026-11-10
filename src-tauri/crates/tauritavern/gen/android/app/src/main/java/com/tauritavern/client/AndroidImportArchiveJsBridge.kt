package com.tauritavern.client

import android.content.ContentResolver
import android.net.Uri
import android.webkit.JavascriptInterface
import java.io.File

class AndroidImportArchiveJsBridge(
  private val contentResolver: ContentResolver,
  private val launchCodexAuthPicker: () -> Unit,
) {
  @JavascriptInterface
  fun requestCodexAuthPicker() {
    launchCodexAuthPicker()
  }

  @JavascriptInterface
  fun stageContentUriToFile(
    contentUri: String?,
    targetPath: String?,
  ): String {
    val uri = Uri.parse(requireNotNull(contentUri).trim())
    val targetFile = File(requireNotNull(targetPath).trim())

    return AndroidContentFileTransfer.copyContentUriToFile(contentResolver, uri, targetFile)
  }

  companion object {
    const val INTERFACE_NAME = "TauriTavernAndroidImportArchiveBridge"
  }
}
