import UIKit
import XCTest

@MainActor
final class WorkNavigationTests: XCTestCase {
  private func launch(largeText: Bool = false) -> XCUIApplication {
    continueAfterFailure = false
    executionTimeAllowance = 600
    let app = XCUIApplication()
    app.launchArguments += ["-notificationsEnabled", "NO", "-coordinatorAddress", ""]
    app.launchArguments += [
      "-UIPreferredContentSizeCategoryName",
      (largeText ? UIContentSizeCategory.accessibilityExtraExtraExtraLarge : .large).rawValue,
    ]
    app.launch()
    addTeardownBlock { @MainActor in app.terminate() }
    if !app.navigationBars["Connection"].waitForExistence(timeout: 3) {
      app.buttons["Connection"].tap()
    }
    XCTAssertTrue(app.navigationBars["Connection"].waitForExistence(timeout: 5))
    app.buttons["dismissConnection"].tap()
    return app
  }

  private func editor(_ app: XCUIApplication) -> XCUIElement {
    app.descendants(matching: .any).matching(identifier: "Message the coordinator").firstMatch
  }

  private func enterUnsentNote(_ text: String, into element: XCUIElement) -> String {
    element.tap()
    element.typeText(text)
    let entered = XCTNSPredicateExpectation(
      predicate: NSPredicate(format: "value CONTAINS %@", text), object: element)
    XCTAssertEqual(XCTWaiter.wait(for: [entered], timeout: 3), .completed)
    let note = element.value as? String ?? ""
    XCTAssertTrue(note.contains(text))
    return note
  }

  private func capture(_ name: String, app: XCUIApplication) {
    let window = app.windows.firstMatch
    XCTAssertEqual(app.state, .runningForeground)
    XCTAssertTrue(window.exists && !window.frame.isEmpty)
    let attachment = XCTAttachment(screenshot: window.screenshot())
    attachment.name = name
    attachment.lifetime = .keepAlways
    add(attachment)
  }

  func testSettingsFromFocusedComposerHidesKeyboardAndKeepsUnsentDraft() {
    let app = launch()
    let composer = editor(app)
    XCTAssertTrue(composer.waitForExistence(timeout: 5))
    app.buttons["composerMenu"].tap()
    app.buttons["Steer"].tap()
    let note = enterUnsentNote(" Keep this note while inspecting settings.", into: composer)
    XCTAssertTrue(app.keyboards.firstMatch.waitForExistence(timeout: 3))
    app.buttons["Connection"].tap()
    XCTAssertTrue(app.navigationBars["Connection"].waitForExistence(timeout: 5))
    XCTAssertTrue(app.keyboards.firstMatch.waitForNonExistence(timeout: 3), app.debugDescription)
    capture("settings-after-focused-composer", app: app)
    app.buttons["dismissConnection"].tap()
    XCTAssertEqual(composer.value as? String, note)
    XCTAssertFalse(app.buttons["Send message"].isEnabled)
    app.buttons["composerMenu"].tap()
    XCTAssertTrue(app.buttons["Steer"].waitForExistence(timeout: 3))
    XCTAssertTrue(app.buttons["Follow-up"].exists)
    capture("native-composer-actions", app: app)
    app.buttons["Follow-up"].tap()
    XCTAssertEqual(composer.value as? String, note)
    XCTAssertFalse(app.buttons["Queue follow-up"].isEnabled)
  }

  func testLargeTextComposerActionsRemainReachableWithoutSending() {
    let app = launch(largeText: true)
    let composer = editor(app)
    XCTAssertTrue(composer.waitForExistence(timeout: 5))
    app.buttons["composerMenu"].tap()
    XCTAssertTrue(app.buttons["Steer"].waitForExistence(timeout: 3))
    app.buttons["Steer"].tap()
    let note = enterUnsentNote(" A note at the largest text size.", into: composer)
    let menu = app.buttons["composerMenu"]
    XCTAssertTrue(menu.isHittable)
    menu.tap()
    XCTAssertTrue(app.buttons["Follow-up"].waitForExistence(timeout: 3))
    capture("large-type-native-composer-actions", app: app)
    app.buttons["Follow-up"].tap()
    XCTAssertEqual(composer.value as? String, note)
    XCTAssertFalse(app.buttons["Queue follow-up"].isEnabled)
  }
}
