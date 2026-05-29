import http from "k6/http";
import { check, sleep } from "k6";

export const options = {
  vus: 10,
  duration: "30s",
  thresholds: {
    http_req_failed: ["rate<0.01"],
    http_req_duration: ["p(95)<1500"],
  },
};

export default function () {
  const now = new Date();
  const eventId = `load_event_${__VU}_${__ITER}_${Date.now()}`;
  const payload = JSON.stringify({
    event_id: eventId,
    user_id: `load_user_${__VU}_${__ITER}`,
    movie_id: "load_movie_ci",
    event_type: "VIEW_STARTED",
    timestamp: now.toISOString(),
    device_type: "MOBILE",
    session_id: `load_session_${__VU}_${__ITER}`,
    progress_seconds: 0,
  });

  const response = http.post("http://localhost:8080/events", payload, {
    headers: {
      "Content-Type": "application/json",
    },
  });

  check(response, {
    "status is 200": (r) => r.status === 200,
    "event accepted": (r) => r.body.includes("accepted"),
  });

  sleep(1);
}
