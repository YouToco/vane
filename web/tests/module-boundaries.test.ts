import { describe, expect, it } from "vitest";
import App from "@/app/App";
import AuthenticatedApp from "@/app/AuthenticatedApp";
import Landing from "@/pages/Landing";
import TaskDetail from "@/pages/TaskDetail";

describe("production module boundaries", () => {
  it("loads app, authenticated shell, and lazy route owners", () => {
    expect(App).toBeTypeOf("function");
    expect(AuthenticatedApp).toBeTypeOf("function");
    expect(Landing).toBeTypeOf("function");
    expect(TaskDetail).toBeTypeOf("function");
  });
});
