#if os(iOS)
  import SwiftUI
  import UIKit
  import OSLog

  /// The Send button clears this native buffer in the same event as its snapshot.
  /// Persistence and delivery belong to ChatModel and the durable outbox.
  @MainActor final class ComposerEditor {
    weak var view: UITextView?
    private let logger = Logger(subsystem: "dev.jessfraz.Agent", category: "Composer")

    @discardableResult
    func submit(_ accept: (String) -> Bool) -> Bool {
      guard let view else { return false }
      view.unmarkText()
      guard accept(view.text ?? "") else { return false }
      view.text = ""
      view.delegate?.textViewDidChange?(view)
      view.undoManager?.removeAllActions()
      view.invalidateIntrinsicContentSize()
      logger.notice(
        "event=nativeBufferCleared characters=\(view.text.count) marked=\(view.markedTextRange != nil) focused=\(view.isFirstResponder)"
      )
      return true
    }
  }

  struct ComposerTextView: UIViewRepresentable {
    @Binding var text: String
    @Binding var focused: Bool
    let enabled: Bool
    let editor: ComposerEditor

    func makeUIView(context: Context) -> UITextView {
      let view = UITextView()
      view.font = .preferredFont(forTextStyle: .body)
      view.adjustsFontForContentSizeCategory = true
      view.textColor = .label
      view.backgroundColor = .clear
      view.textContainerInset = .zero
      view.textContainer.lineFragmentPadding = 0
      view.text = text
      view.setContentCompressionResistancePriority(.defaultLow, for: .horizontal)
      view.accessibilityLabel = "Message the coordinator"
      view.accessibilityIdentifier = "Message the coordinator"
      view.delegate = context.coordinator
      editor.view = view
      return view
    }

    func updateUIView(_ view: UITextView, context: Context) {
      guard !context.coordinator.updating else { return }
      context.coordinator.parent = self
      context.coordinator.updating = true
      defer { context.coordinator.updating = false }
      if view.isEditable != enabled { view.isEditable = enabled }
      // Apply external changes, not an echo of the editor's own binding. A
      // focus update can arrive before the keyboard publishes marked text.
      if context.coordinator.lastText != text {
        context.coordinator.logger.notice(
          "event=nativeExternalText characters=\(view.text.count) replacementCharacters=\(text.count) marked=\(view.markedTextRange != nil)"
        )
        context.coordinator.lastText = text
        if view.text != text {
          view.unmarkText()
          view.text = text
          view.invalidateIntrinsicContentSize()
        }
      }
      if focused, enabled, !view.isFirstResponder {
        view.becomeFirstResponder()
      } else if !focused, view.isFirstResponder {
        view.resignFirstResponder()
      }
    }

    func sizeThatFits(_ proposal: ProposedViewSize, uiView: UITextView, context: Context) -> CGSize?
    {
      guard let width = proposal.width, let font = uiView.font else { return nil }
      let height = uiView.sizeThatFits(CGSize(width: width, height: .greatestFiniteMagnitude))
        .height
      return CGSize(width: width, height: min(max(height, font.lineHeight), font.lineHeight * 6))
    }

    func makeCoordinator() -> Coordinator { Coordinator(self) }

    static func dismantleUIView(_ view: UITextView, coordinator: Coordinator) {
      view.delegate = nil
      if coordinator.parent.editor.view === view { coordinator.parent.editor.view = nil }
    }

    final class Coordinator: NSObject, UITextViewDelegate {
      var parent: ComposerTextView
      var updating = false
      var lastText: String
      let logger = Logger(subsystem: "dev.jessfraz.Agent", category: "Composer")
      init(_ parent: ComposerTextView) {
        self.parent = parent
        lastText = parent.text
      }

      func textViewDidChange(_ textView: UITextView) {
        guard !updating else { return }
        updating = true
        defer { updating = false }
        lastText = textView.text
        if parent.text != lastText { parent.text = lastText }
        textView.invalidateIntrinsicContentSize()
      }

      func textViewDidBeginEditing(_ textView: UITextView) {
        if !updating { parent.focused = true }
      }

      func textViewDidEndEditing(_ textView: UITextView) {
        if !updating { parent.focused = false }
      }
    }
  }
#endif
