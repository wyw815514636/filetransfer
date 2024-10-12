package org.sync.transfer.http

import io.ktor.http.*
import io.ktor.http.content.*
import io.ktor.server.application.*
import io.ktor.server.engine.*
import io.ktor.server.netty.*
import io.ktor.server.request.*
import io.ktor.server.response.*
import io.ktor.server.routing.*
import io.ktor.util.pipeline.*
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.launch
import org.sync.transfer.calculateMD5
import java.io.File
import java.util.concurrent.ConcurrentHashMap
import kotlin.time.Duration.Companion.minutes

// 使用 Map 存储块信息
private val uploadedChunks = ConcurrentHashMap<String, String>()

// 存储当前正在传输的小文件的 MD5 值
private val processingSmallFiles = ConcurrentHashMap.newKeySet<String>()

// 存储当前正在传输的大文件的 MD5 值
private val processingLargeFiles = ConcurrentHashMap.newKeySet<String>()

// 存储小文件的最后一次传输时间
private val smallFileLastActiveTimes = ConcurrentHashMap<String, Long>()

// 小文件超时时间
private val smallFileTimeout = 1.minutes

// 限制同时传输文件的数量
private const val MAX_CONCURRENT_SMALL_FILES = 20
private const val MAX_CONCURRENT_LARGE_FILES = 5

fun startHttpServer(port: Int) {

    // 初始化 uploadedChunks
    val tempAllDir = File("temp")
    if (tempAllDir.exists()) {
        tempAllDir.walkTopDown().forEach { file ->
            if (file.isFile) {
                val parts= file.name.split(".")
                if (parts.size == 2) {
                    val filename = parts[0]
                    val chunkIndex = parts[1]
                    uploadedChunks["$filename.$chunkIndex"] = file.calculateMD5()
                }
            }
        }
    }

    GlobalScope.launch(Dispatchers.IO) {

        embeddedServer(Netty, port = port) {
            intercept(ApplicationCallPipeline.Call) {
                // 只拦截 /upload 接口
                if (call.request.uri.startsWith("/upload_small") || call.request.uri.startsWith("/upload_large")) {
                    // 获取文件的 MD5 值
                    val fileMd5 = call.parameters["fileMd5"]

                    if (fileMd5 != null) {
                        if (call.request.uri.startsWith("/upload_small")) {
                            handleSmallFileUpload(fileMd5)
                        } else if (call.request.uri.startsWith("/upload_large")) {
                            handleLargeFileUpload(fileMd5)
                        }
                    } else {
                        // fileMd5 为空，返回错误响应
                        call.respond(HttpStatusCode.BadRequest, "Invalid request parameters")
                    }
                } else {
                    proceed()
                }
            }
            routing {
                get("/hello") {call.respondText("Hello from Kotlin Multiplatform with ktor!")
                }

                post("/check_file/{filename}/{fileMd5}") {
                    val filename = call.parameters["filename"]!!
                    val receivedFileMd5 = call.parameters["fileMd5"]!!

                    val file = File("uploads", filename)
                    if (file.exists() && file.calculateMD5() == receivedFileMd5) {
                        call.respondText("File already exists")
                    } else {
                        call.respondText("File does not exist")
                    }
                }

                post("/verify/{filename}/{fileMd5}") {
                    val filename = call.parameters["filename"]!!
                    val receivedFileMd5 = call.parameters["fileMd5"]!!

                    var finalFile = File("uploads", filename)
                    if (finalFile.exists()) {
                        // 如果文件已存在，则校验 MD5
                        if (finalFile.calculateMD5() == receivedFileMd5) {
                            // MD5相同，文件已存在，直接返回成功
                            call.respondText("File verification successful")
                            return@post
                        } else {
                            // 如果 MD5 不同，则添加后缀
                            val fileExtension = filename.substringAfterLast('.', "")
                            val fileNameWithoutExtension = filename.substringBeforeLast('.', "")
                            finalFile = File("uploads", "$fileNameWithoutExtension-${System.currentTimeMillis()}.$fileExtension")
                        }} else {
                        // 文件不存在，创建新文件
                        finalFile.createNewFile()
                    }

                    val tempDir = File("temp", receivedFileMd5)
                    if (tempDir.exists()) {
                        // 合并临时文件
                        finalFile.outputStream().use { output ->
                            val chunks = tempDir.listFiles()?.sortedBy { it.nameWithoutExtension.toInt() }
                            chunks?.forEach { chunkFile ->
                                chunkFile.inputStream().use { input ->
                                    input.copyTo(output)
                                }
                            }
                        }
                    }

                    if (receivedFileMd5 == finalFile.calculateMD5()) {
                        processingLargeFiles.remove(receivedFileMd5)
                        processingSmallFiles.remove(receivedFileMd5)
                        uploadedChunks.keys.filter { it.startsWith(filename) }.forEach {
                            uploadedChunks.remove(it)
                        }
                        // 校验成功后，删除临时文件和文件夹
                        if (tempDir.exists()) {
                            tempDir.deleteRecursively()
                        }
                        call.respondText("File verification successful")

                    } else {
                        // 如果校验失败，删除文件
                        finalFile.delete()
                        call.respondText("File verification failed", status = HttpStatusCode.BadRequest)
                    }
                }

                // 处理小文件上传
                post("/upload_small/{fileMd5}/{filename}/{chunkIndex}/{chunkMd5}") {
                    handleFileUpload()

                    // 更新最后一次传输时间
                    val fileMd5 = call.parameters["fileMd5"]!!
                    smallFileLastActiveTimes[fileMd5] = System.currentTimeMillis()
                }

                // 处理大文件上传
                post("/upload_large/{fileMd5}/{filename}/{chunkIndex}/{chunkMd5}") {
                    handleFileUpload()
                }


            }
        }.start(wait = true)
    }
}

// 处理文件上传的公共逻辑
private suspend fun PipelineContext<Unit, ApplicationCall>.handleFileUpload() {
    val fileMd5 = call.parameters["fileMd5"]!!
    val filename = call.parameters["filename"]!!
    val chunkIndex = call.parameters["chunkIndex"]!!.toInt()
    val chunkMd5 = call.parameters["chunkMd5"]!!

    if (uploadedChunks["$filename.$chunkIndex"] == chunkMd5) {
        call.respond(HttpStatusCode.OK, "Chunk $chunkIndex already uploaded")
        return
    }

    val multipartData = call.receiveMultipart()
    var fileBytes: ByteArray? = null
    multipartData.forEachPart { part ->
        if (part is PartData.FileItem) {
            fileBytes = part.streamProvider().readBytes()
        }
    }

    val tempDir = File("temp", fileMd5)
    tempDir.mkdirs()
    val tempFile = File(tempDir, "$filename.$chunkIndex")
    tempFile.writeBytes(fileBytes!!)

    if (chunkMd5 != tempFile.calculateMD5()) {
        tempFile.delete() // 删除无效的块文件
        call.respondText("Chunk verification failed", status = HttpStatusCode.BadRequest)
        return
    }

    uploadedChunks["$filename.$chunkIndex"] = chunkMd5
    call.respondText("Chunk $chunkIndex received")
}

// 处理小文件上传请求
private suspend fun PipelineContext<Unit, ApplicationCall>.handleSmallFileUpload(fileMd5: String) {
    val currentTime = System.currentTimeMillis()

    if (processingSmallFiles.size >= MAX_CONCURRENT_SMALL_FILES && !processingSmallFiles.contains(fileMd5)) {
        // 检查是否有超时的小文件
        val timeoutFileMd5 = smallFileLastActiveTimes.entries.find {
            currentTime - it.value > smallFileTimeout.inWholeMilliseconds
        }?.key

        if (timeoutFileMd5 != null) {
            processingSmallFiles.remove(timeoutFileMd5)
            smallFileLastActiveTimes.remove(timeoutFileMd5)
        } else {
            call.respond(HttpStatusCode.ServiceUnavailable, "Server is busy, please try again later.")
            return
        }
    }

    processingSmallFiles.add(fileMd5)
    smallFileLastActiveTimes[fileMd5] = currentTime

    try {
        proceed()
    } finally {
        processingSmallFiles.remove(fileMd5)
    }
}

// 处理大文件上传请求
private suspend fun PipelineContext<Unit, ApplicationCall>.handleLargeFileUpload(fileMd5: String) {
    if (processingLargeFiles.size >= MAX_CONCURRENT_LARGE_FILES && !processingLargeFiles.contains(fileMd5)) {
        call.respond(HttpStatusCode.ServiceUnavailable, "Server is busy, please try again later.")
        return
    }
    processingLargeFiles.add(fileMd5)
    try {
        proceed()
    } finally {
        processingLargeFiles.remove(fileMd5)
    }
}
