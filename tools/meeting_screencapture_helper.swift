import AVFoundation
import CoreMedia
import Foundation
import ScreenCaptureKit

final class AudioFileSink {
    private let url: URL
    private var file: AVAudioFile?
    private var sampleCount: Int64 = 0

    init(url: URL) {
        self.url = url
    }

    func write(_ sampleBuffer: CMSampleBuffer) throws {
        guard sampleBuffer.isValid, CMSampleBufferDataIsReady(sampleBuffer) else { return }
        guard let formatDescription = CMSampleBufferGetFormatDescription(sampleBuffer),
              let streamDescription = CMAudioFormatDescriptionGetStreamBasicDescription(formatDescription) else {
            return
        }
        var asbd = streamDescription.pointee
        guard let format = AVAudioFormat(streamDescription: &asbd) else { return }
        let frameCount = CMSampleBufferGetNumSamples(sampleBuffer)
        guard frameCount > 0,
              let buffer = AVAudioPCMBuffer(pcmFormat: format, frameCapacity: AVAudioFrameCount(frameCount)) else {
            return
        }
        buffer.frameLength = AVAudioFrameCount(frameCount)
        CMSampleBufferCopyPCMDataIntoAudioBufferList(
            sampleBuffer,
            at: 0,
            frameCount: Int32(frameCount),
            into: buffer.mutableAudioBufferList
        )
        if file == nil {
            file = try AVAudioFile(forWriting: url, settings: format.settings)
        }
        try file?.write(from: buffer)
        sampleCount += Int64(frameCount)
    }

    var hasAudio: Bool {
        sampleCount > 0
    }
}

final class RawPCMSink {
    private let output = FileHandle.standardOutput
    private let outputLock = NSLock()
    private let targetFormat = AVAudioFormat(commonFormat: .pcmFormatInt16, sampleRate: 24_000, channels: 1, interleaved: true)!

    func write(_ sampleBuffer: CMSampleBuffer) throws {
        guard sampleBuffer.isValid, CMSampleBufferDataIsReady(sampleBuffer) else { return }
        guard let formatDescription = CMSampleBufferGetFormatDescription(sampleBuffer),
              let streamDescription = CMAudioFormatDescriptionGetStreamBasicDescription(formatDescription) else {
            return
        }
        var asbd = streamDescription.pointee
        guard let sourceFormat = AVAudioFormat(streamDescription: &asbd) else { return }
        let frameCount = CMSampleBufferGetNumSamples(sampleBuffer)
        guard frameCount > 0,
              let sourceBuffer = AVAudioPCMBuffer(pcmFormat: sourceFormat, frameCapacity: AVAudioFrameCount(frameCount)) else {
            return
        }
        sourceBuffer.frameLength = AVAudioFrameCount(frameCount)
        CMSampleBufferCopyPCMDataIntoAudioBufferList(
            sampleBuffer,
            at: 0,
            frameCount: Int32(frameCount),
            into: sourceBuffer.mutableAudioBufferList
        )
        guard let converter = AVAudioConverter(from: sourceFormat, to: targetFormat) else { return }
        let ratio = targetFormat.sampleRate / max(1, sourceFormat.sampleRate)
        let outputCapacity = AVAudioFrameCount(Double(frameCount) * ratio) + 1024
        guard let outputBuffer = AVAudioPCMBuffer(pcmFormat: targetFormat, frameCapacity: outputCapacity) else { return }
        var didProvideInput = false
        var conversionError: NSError?
        converter.convert(to: outputBuffer, error: &conversionError) { _, status in
            if didProvideInput {
                status.pointee = .noDataNow
                return nil
            }
            didProvideInput = true
            status.pointee = .haveData
            return sourceBuffer
        }
        if let conversionError {
            throw conversionError
        }
        guard outputBuffer.frameLength > 0, let channel = outputBuffer.int16ChannelData?[0] else { return }
        let byteCount = Int(outputBuffer.frameLength) * MemoryLayout<Int16>.size
        let data = Data(bytes: channel, count: byteCount)
        outputLock.lock()
        output.write(data)
        outputLock.unlock()
    }
}

final class CaptureOutput: NSObject, SCStreamOutput, SCStreamDelegate {
    let systemSink: AudioFileSink
    let microphoneSink: AudioFileSink
    private let rawPCMSink: RawPCMSink?
    private let statusURL: URL
    private let statusEveryBuffers: Int
    private let lock = NSLock()
    private var systemBuffers = 0
    private var microphoneBuffers = 0

    init(systemURL: URL, microphoneURL: URL, statusURL: URL, statusEveryBuffers: Int, rawPCMSink: RawPCMSink?) {
        self.systemSink = AudioFileSink(url: systemURL)
        self.microphoneSink = AudioFileSink(url: microphoneURL)
        self.rawPCMSink = rawPCMSink
        self.statusURL = statusURL
        self.statusEveryBuffers = max(1, statusEveryBuffers)
    }

    func stream(_ stream: SCStream, didOutputSampleBuffer sampleBuffer: CMSampleBuffer, of type: SCStreamOutputType) {
        do {
            switch type {
            case .audio:
                try systemSink.write(sampleBuffer)
                try rawPCMSink?.write(sampleBuffer)
                increment(system: true)
            case .microphone:
                try microphoneSink.write(sampleBuffer)
                try rawPCMSink?.write(sampleBuffer)
                increment(system: false)
            default:
                break
            }
        } catch {
            writeStatus("warning", "audio_write_failed: \(error.localizedDescription)")
        }
    }

    func stream(_ stream: SCStream, didStopWithError error: Error) {
        writeStatus("failed", "stream_stopped: \(error.localizedDescription)")
        CFRunLoopStop(CFRunLoopGetMain())
    }

    private func increment(system: Bool) {
        lock.lock()
        if system {
            systemBuffers += 1
        } else {
            microphoneBuffers += 1
        }
        let currentSystem = systemBuffers
        let currentMicrophone = microphoneBuffers
        lock.unlock()
        if (currentSystem + currentMicrophone) % statusEveryBuffers == 0 {
            writeStatus("recording", "system_buffers=\(currentSystem), microphone_buffers=\(currentMicrophone)")
        }
    }

    func writeStatus(_ status: String, _ message: String) {
        let payload: [String: Any] = [
            "status": status,
            "message": message,
            "system_buffers": systemBuffers,
            "microphone_buffers": microphoneBuffers,
            "updated_at": ISO8601DateFormatter().string(from: Date()),
        ]
        if let data = try? JSONSerialization.data(withJSONObject: payload, options: []) {
            try? data.write(to: statusURL)
        }
    }
}

struct Arguments {
    let systemOutput: URL
    let microphoneOutput: URL
    let statusOutput: URL
    let statusEveryBuffers: Int
    let rawPCMOutput: String?

    init() throws {
        var values: [String: String] = [:]
        var iterator = CommandLine.arguments.dropFirst().makeIterator()
        while let key = iterator.next() {
            guard key.hasPrefix("--"), let value = iterator.next() else { continue }
            values[String(key.dropFirst(2))] = value
        }
        guard let system = values["system-output"],
              let microphone = values["microphone-output"],
              let status = values["status-output"] else {
            throw NSError(domain: "MeetingRecorder", code: 2, userInfo: [
                NSLocalizedDescriptionKey: "Usage: meeting-screencapture-helper --system-output system.caf --microphone-output microphone.caf --status-output status.json [--status-every-buffers 250]"
            ])
        }
        self.systemOutput = URL(fileURLWithPath: system)
        self.microphoneOutput = URL(fileURLWithPath: microphone)
        self.statusOutput = URL(fileURLWithPath: status)
        self.statusEveryBuffers = max(1, Int(values["status-every-buffers"] ?? "250") ?? 250)
        self.rawPCMOutput = values["raw-pcm-output"]
    }
}

@main
struct MeetingScreenCaptureHelper {
    static func main() async {
        do {
            let args = try Arguments()
            try FileManager.default.createDirectory(at: args.systemOutput.deletingLastPathComponent(), withIntermediateDirectories: true)

            let content = try await SCShareableContent.excludingDesktopWindows(false, onScreenWindowsOnly: false)
            guard let display = content.displays.first else {
                throw NSError(domain: "MeetingRecorder", code: 3, userInfo: [NSLocalizedDescriptionKey: "No display is available for ScreenCaptureKit audio capture."])
            }

            let filter = SCContentFilter(display: display, excludingWindows: [])
            let configuration = SCStreamConfiguration()
            configuration.width = 2
            configuration.height = 2
            configuration.minimumFrameInterval = CMTime(value: 1, timescale: 1)
            configuration.capturesAudio = true
            configuration.sampleRate = 48_000
            configuration.channelCount = 2
            configuration.excludesCurrentProcessAudio = true
            if #available(macOS 15.0, *) {
                configuration.captureMicrophone = true
            }

            let output = CaptureOutput(
                systemURL: args.systemOutput,
                microphoneURL: args.microphoneOutput,
                statusURL: args.statusOutput,
                statusEveryBuffers: args.statusEveryBuffers,
                rawPCMSink: args.rawPCMOutput == "stdout" ? RawPCMSink() : nil
            )
            let stream = SCStream(filter: filter, configuration: configuration, delegate: output)
            let queue = DispatchQueue(label: "meeting-recorder.screencapture.audio")
            try stream.addStreamOutput(output, type: .audio, sampleHandlerQueue: queue)
            if #available(macOS 15.0, *) {
                try stream.addStreamOutput(output, type: .microphone, sampleHandlerQueue: queue)
            }

            signal(SIGTERM, SIG_IGN)
            signal(SIGINT, SIG_IGN)
            let signalQueue = DispatchQueue(label: "meeting-recorder.screencapture.signals")
            let term = DispatchSource.makeSignalSource(signal: SIGTERM, queue: signalQueue)
            let interrupt = DispatchSource.makeSignalSource(signal: SIGINT, queue: signalQueue)

            output.writeStatus("starting", "starting ScreenCaptureKit audio capture")
            try await stream.startCapture()
            output.writeStatus("recording", "ScreenCaptureKit audio capture started")
            await withCheckedContinuation { (continuation: CheckedContinuation<Void, Never>) in
                let resumeLock = NSLock()
                var didResume = false
                let resumeOnce = {
                    resumeLock.lock()
                    if didResume {
                        resumeLock.unlock()
                        return
                    }
                    didResume = true
                    resumeLock.unlock()
                    continuation.resume()
                }
                term.setEventHandler(handler: resumeOnce)
                interrupt.setEventHandler(handler: resumeOnce)
                term.resume()
                interrupt.resume()
            }
            try await stream.stopCapture()
            output.writeStatus("stopped", "ScreenCaptureKit audio capture stopped")
        } catch {
            let message = error.localizedDescription
            if let args = try? Arguments() {
                let payload: [String: Any] = [
                    "status": "failed",
                    "message": message,
                    "updated_at": ISO8601DateFormatter().string(from: Date()),
                ]
                if let data = try? JSONSerialization.data(withJSONObject: payload, options: []) {
                    try? data.write(to: args.statusOutput)
                }
            }
            FileHandle.standardError.write(Data((message + "\n").utf8))
            exit(1)
        }
    }
}
