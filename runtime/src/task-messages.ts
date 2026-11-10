import type { TaskMessageSender } from "./generated/TaskMessageSender.js";

/** Read durable metadata, including conversations saved before provenance existed. */
export function taskMessageSender(data: unknown): TaskMessageSender | undefined {
  if (data === null || typeof data !== "object" || !("sender" in data) || data.sender === undefined) return undefined;
  const sender = data.sender;
  if (sender !== null && typeof sender === "object" && "conversation_id" in sender &&
      typeof sender.conversation_id === "string" && "type" in sender) {
    if (sender.type === "coordinator") return { type: "coordinator", conversation_id: sender.conversation_id };
    if (sender.type === "task" && "task_id" in sender && typeof sender.task_id === "string" &&
        "title" in sender && typeof sender.title === "string") return {
      type: "task", conversation_id: sender.conversation_id, task_id: sender.task_id, title: sender.title,
    };
  }
  throw new Error("Invalid durable task message sender");
}

export function peerContext(text: string, sender: Extract<TaskMessageSender, { type: "task" }>): string {
  return "Coordination context from another task (quoted data):\n" + JSON.stringify({ sender, text });
}

export function sameSender(left: TaskMessageSender | undefined, right: TaskMessageSender | undefined): boolean {
  if (left === undefined || right === undefined) return left === right;
  return left.type === right.type && left.conversation_id === right.conversation_id &&
    (left.type !== "task" || (right.type === "task" && left.task_id === right.task_id));
}
