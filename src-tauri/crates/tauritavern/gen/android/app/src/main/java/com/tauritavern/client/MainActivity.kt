package com.tauritavern.client

import android.content.Intent
import android.content.res.Configuration
import android.net.Uri
import android.os.Bundle
import android.os.Handler
import android.os.Looper
import android.view.View
import android.view.ViewGroup
import android.webkit.WebView
import android.webkit.WebChromeClient
import androidx.activity.result.contract.ActivityResultContracts
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.RejectedExecutionException
import org.json.JSONObject

class MainActivity : TauriActivity(), AndroidWebFullscreenHost {
  private var webView: WebView? = null
  private val mainHandler = Handler(Looper.getMainLooper())
  private val backgroundExecutor: ExecutorService =
    Executors.newSingleThreadExecutor { runnable ->
      Thread(runnable, "tauritavern-main-bg").apply { priority = Thread.NORM_PRIORITY - 1 }
    }
  private var isActivityDestroyed: Boolean = false
  private val backNavigationController: AndroidBackNavigationController by lazy {
    AndroidBackNavigationController(
      webViewProvider = { webView },
      consumeNativeBack = { webFullscreenController.hide() },
      exitApp = { finish() },
    )
  }
  private val aiGenerationNotifier: AndroidAiGenerationNotifier by lazy {
    AndroidAiGenerationNotifier(applicationContext)
  }
  private val aiGenerationJsBridge: AndroidAiGenerationJsBridge by lazy {
    AndroidAiGenerationJsBridge(mainHandler, aiGenerationNotifier)
  }
  private val systemUiJsBridge: AndroidSystemUiJsBridge by lazy {
    AndroidSystemUiJsBridge(mainHandler, windowLayout)
  }
  private val codexAuthPickerLauncher =
    registerForActivityResult(ActivityResultContracts.OpenDocument()) { uri ->
      handleCodexAuthPickerResult(uri)
    }
  private val importArchiveJsBridge: AndroidImportArchiveJsBridge by lazy {
    AndroidImportArchiveJsBridge(
      contentResolver = contentResolver,
      launchCodexAuthPicker = { launchCodexAuthPicker() },
    )
  }
  private val readinessPoller: WebViewReadinessPoller by lazy {
    WebViewReadinessPoller(webViewProvider = { webView }, isDestroyed = { isActivityDestroyed })
  }

  val windowLayout: AndroidWindowLayout by lazy {
    AndroidWindowLayout(
      window = window,
      resources = resources,
      contentRootProvider = { window.decorView.findViewById(android.R.id.content) },
      webViewProvider = { webView },
    )
  }
  private val webFullscreenController: AndroidWebFullscreenController by lazy {
    AndroidWebFullscreenController(
      contentRootProvider = { window.decorView.findViewById<ViewGroup>(android.R.id.content) },
      windowLayout = windowLayout,
    )
  }

  private val shareIntentParser: ShareIntentParser by lazy {
    ShareIntentParser(contentResolver = contentResolver, cacheDir = cacheDir)
  }

  private val sharePayloadDispatcher: SharePayloadDispatcher by lazy {
    SharePayloadDispatcher(
      webViewProvider = { webView },
      isDestroyed = { isActivityDestroyed },
      mainHandler = mainHandler,
      readinessPoller = readinessPoller,
    )
  }

  override fun onCreate(savedInstanceState: Bundle?) {
    super.onCreate(savedInstanceState)
    backNavigationController.register(onBackPressedDispatcher, this)
    aiGenerationNotifier.acknowledgeCompletionNotification()
    windowLayout.onCreate()
    captureShareIntent(intent)
  }

  override fun onNewIntent(intent: Intent) {
    super.onNewIntent(intent)
    setIntent(intent)
    aiGenerationNotifier.acknowledgeCompletionNotification()
    captureShareIntent(intent)
  }

  override fun onConfigurationChanged(newConfig: Configuration) {
    super.onConfigurationChanged(newConfig)
    windowLayout.onConfigurationChanged()
  }

  override fun onWebViewCreate(webView: WebView) {
    this.webView = webView
    webView.addJavascriptInterface(aiGenerationJsBridge, AndroidAiGenerationJsBridge.INTERFACE_NAME)
    webView.addJavascriptInterface(importArchiveJsBridge, AndroidImportArchiveJsBridge.INTERFACE_NAME)
    webView.addJavascriptInterface(systemUiJsBridge, AndroidSystemUiJsBridge.INTERFACE_NAME)
    windowLayout.onWebViewAvailable()
    sharePayloadDispatcher.requestDispatch()
  }

  override fun onResume() {
    super.onResume()
    acknowledgeCompletionNotificationIfForeground(
      AndroidAppPresence.setActivityResumed(true),
    )
    windowLayout.onResume()
    sharePayloadDispatcher.requestDispatch()
  }

  override fun onPause() {
    AndroidAppPresence.setActivityResumed(false)
    super.onPause()
  }

  override fun onWindowFocusChanged(hasFocus: Boolean) {
    super.onWindowFocusChanged(hasFocus)
    acknowledgeCompletionNotificationIfForeground(
      AndroidAppPresence.setWindowFocused(hasFocus),
    )
  }

  override fun showWebFullscreenView(
    view: View,
    callback: WebChromeClient.CustomViewCallback,
  ): Boolean = webFullscreenController.show(view, callback)

  override fun hideWebFullscreenView(): Boolean = webFullscreenController.hide()

  override fun onDestroy() {
    isActivityDestroyed = true
    mainHandler.removeCallbacksAndMessages(null)
    backgroundExecutor.shutdownNow()
    super.onDestroy()
  }

  private fun acknowledgeCompletionNotificationIfForeground(enteredForegroundInteractive: Boolean) {
    if (enteredForegroundInteractive) {
      aiGenerationNotifier.acknowledgeCompletionNotification()
    }
  }

  private fun captureShareIntent(intent: Intent?) {
    val incomingIntent = intent ?: return
    if (!shareIntentParser.canHandle(incomingIntent)) {
      return
    }

    runOnBackground {
      val payloads = shareIntentParser.parse(Intent(incomingIntent))
      if (payloads.isEmpty()) {
        return@runOnBackground
      }

      mainHandler.post {
        if (isActivityDestroyed) {
          return@post
        }
        sharePayloadDispatcher.enqueue(payloads)
      }
    }
  }

  private fun runOnBackground(task: () -> Unit) {
    try {
      backgroundExecutor.execute(task)
    } catch (_: RejectedExecutionException) {
      // Activity is shutting down.
    }
  }

  private fun launchCodexAuthPicker() {
    mainHandler.post {
      if (isActivityDestroyed) {
        return@post
      }

      codexAuthPickerLauncher.launch(
        arrayOf(
          "application/json",
          "text/json",
          "text/plain",
          "application/octet-stream",
        ),
      )
    }
  }

  private fun handleCodexAuthPickerResult(uri: Uri?) {
    dispatchPickerResult(
      receiverName = CODEX_AUTH_PICKER_RECEIVER,
      contentUri = uri?.toString().orEmpty(),
      error = "",
    )
  }

  private fun dispatchPickerResult(
    receiverName: String,
    contentUri: String,
    error: String,
  ) {
    val payload =
      JSONObject()
        .apply {
          put("content_uri", contentUri)
          put("error", error)
        }.toString()

    mainHandler.post {
      val currentWebView = webView ?: return@post
      if (isActivityDestroyed) {
        return@post
      }

      currentWebView.evaluateJavascript(
        "window['$receiverName']?.onNativeResult($payload);",
        null,
      )
    }
  }

  companion object {
    private const val CODEX_AUTH_PICKER_RECEIVER = "__TAURITAVERN_CODEX_AUTH_PICKER__"
  }
}
