package org.sync.transfer.http

import io.ktor.client.*
import io.ktor.client.engine.cio.*
import io.ktor.client.request.*
import io.ktor.client.request.forms.*
import io.ktor.client.statement.*
import io.ktor.http.*
import kotlinx.coroutines.*
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.sync.withPermit
import org.sync.transfer.calculateMD5
import java.io.File

private const val UPLOAD_SEMAPHORE = 5

suspend fun uploadFile(file: File, serverUrl: String) {
    val client = HttpClient(CIO)
    // 根据文件大小动态调整 chunkSize
    val chunkSize = when {
        file.length() < 100 * 1024 * 1024 -> 16 * 1024 * 1024 // 小于 100MB，使用 16MB 的 chunkSize
        file.length() < 500 * 1024 * 1024 -> 32 * 1024 * 1024 // 100MB 到 500MB，使用 32MB 的 chunkSize
        else -> 64 * 1024 * 1024 // 大于 500MB，使用 64MB 的 chunkSize
    }
    val totalChunks = (file.length() / chunkSize).toInt() + if (file.length()% chunkSize == 0L) 0 else 1

    // 检查文件大小，如果大于 10MB 则请求 /check_file 接口
    if (file.length() > 10 * 1024 * 1024) {
        val response = client.post("$serverUrl/check_file/${file.name}/${file.calculateMD5()}")
        if (response.bodyAsText() == "File already exists") {
            println("File already exists on server, skipping upload")
            return
        }
    }

    // 根据文件大小选择上传接口
    val uploadUrl = if (file.length() < 100 * 1024 * 1024) {
        "$serverUrl/upload_small/${file.calculateMD5()}/${file.name}"
    } else {
        "$serverUrl/upload_large/${file.calculateMD5()}/${file.name}"
    }

    // 创建 Semaphore，限制并发数量为 5
    val semaphore = Semaphore(UPLOAD_SEMAPHORE)

    var uploadedChunks = 0
    val jobs = List(totalChunks) { chunkIndex ->
        GlobalScope.launch {
            // 获取 Semaphore 许可
            semaphore.withPermit {
                val start = chunkIndex * chunkSize
                val end = minOf((start + chunkSize).toLong(), file.length()).toInt()
                val chunkData = file.readBytes().sliceArray(start until end)

                val chunkMd5 = File.createTempFile("chunk", null).apply {
                    writeBytes(chunkData)
                }.calculateMD5()

                try {
                    val response = client.post("$uploadUrl/$chunkIndex/$chunkMd5") {
                        setBody(MultiPartFormDataContent(formData {
                            append("file", chunkData, Headers.build {
                                append(HttpHeaders.ContentType, ContentType.Application.OctetStream)
                                append(HttpHeaders.ContentDisposition, "filename=\"chunk_$chunkIndex\"")
                            })
                        }))
                    }
                    if (response.status.isSuccess()) {
                        uploadedChunks++
                        val progress = (uploadedChunks.toFloat() / totalChunks * 100).toInt()
                        println("Chunk $chunkIndex uploaded: ${response.status} ($progress%)")
                    } else {
                        println("Error uploading chunk $chunkIndex: ${response.status}")
                    }
                } catch (e: Exception) {
                    println("Error uploading chunk $chunkIndex: ${e.message}")
                }
            }
        }
    }
    jobs.joinAll()
    if (uploadedChunks == totalChunks) {
        try {
            val response = client.post("$serverUrl/verify/${file.name}/${file.calculateMD5()}")
            println("File verification: ${response.status}")
        } catch (e: Exception) {
            println("Error verifying file: ${e.message}")
        }
    } else {
        println("File upload failed")
    }
    client.close()
}