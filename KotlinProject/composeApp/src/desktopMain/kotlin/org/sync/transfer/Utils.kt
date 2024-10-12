package org.sync.transfer

import java.io.File
import java.security.MessageDigest

fun File.calculateMD5(): String {
    val buffer = ByteArray(8192)
    val md = MessageDigest.getInstance("MD5")
    this.inputStream().use{ input ->
        var read: Int
        while (input.read(buffer).also { read = it } != -1) {
            md.update(buffer, 0, read)
        }
    }
    return md.digest().joinToString("") { "%02x".format(it) }
}