import React from "react";
import { cancelTransfer, pauseTransfer, resumeTransfer } from "../api";
import type { TransferSnapshot } from "../types";

export const fmtMB = (bytes: number) => `${(bytes / (1024 * 1024)).toFixed(2)} MB`;
export const fmtRate = (bps: number) => `${(bps / (1024 * 1024)).toFixed(2)} MB/s`;

interface Props {
  transfers: TransferSnapshot[];
  onRefresh: () => void;
  emptyText?: string;
}

export const TransfersTable: React.FC<Props> = ({ transfers, onRefresh, emptyText }) => {
  if (transfers.length === 0) {
    return (
      <p style={{ color: "#94a3b8", margin: 0 }}>
        {emptyText ?? "No transfers yet."}
      </p>
    );
  }

  return (
    <div style={{ overflowX: "auto" }}>
      <table>
        <thead>
          <tr>
            <th>File</th>
            <th>Direction</th>
            <th>Status</th>
            <th>Progress</th>
            <th>Transferred / Remaining</th>
            <th>Speed (Measured)</th>
            <th>Allocated</th>
            <th>Workers</th>
            <th>Controls</th>
          </tr>
        </thead>
        <tbody>
          {transfers.map((t) => (
            <tr key={t.transfer_id}>
              <td>
                <strong>{t.filename}</strong>
                <div style={{ fontSize: "0.73rem", color: "#94a3b8" }}>
                  {t.transfer_id} · {t.completed_chunks}/{t.total_chunks || 1} chunks
                </div>
              </td>
              <td>
                <span className={`badge badge-${t.direction}`}>
                  {t.direction === "upload" ? "↑ UPLOAD" : "↓ DOWNLOAD"}
                </span>
              </td>
              <td>
                <span className={`badge badge-${t.state}`}>{t.state}</span>
              </td>
              <td>
                <div className="progress-track">
                  <div
                    className={`progress-fill ${t.direction}`}
                    style={{ width: `${t.progress * 100}%` }}
                  />
                </div>
                {(t.progress * 100).toFixed(0)}%
              </td>
              <td>
                {fmtMB(t.bytes_transferred)} / {fmtMB(t.remaining_bytes)}
              </td>
              <td>
                {fmtRate(t.measured.rolling_goodput_bps || t.measured.instant_goodput_bps)}
              </td>
              <td>{fmtRate(t.allocated_bps)}</td>
              <td>{t.active_workers}</td>
              <td>
                {t.state === "active" && (
                  <button
                    className="btn-sm"
                    onClick={async () => {
                      await pauseTransfer(t.transfer_id);
                      onRefresh();
                    }}
                  >
                    Pause
                  </button>
                )}
                {t.state === "paused" && (
                  <button
                    className="btn-sm"
                    onClick={async () => {
                      await resumeTransfer(t.transfer_id);
                      onRefresh();
                    }}
                  >
                    Resume
                  </button>
                )}
                {(t.state === "active" || t.state === "paused" || t.state === "pending") && (
                  <button
                    className="btn-danger"
                    onClick={async () => {
                      await cancelTransfer(t.transfer_id);
                      onRefresh();
                    }}
                  >
                    Cancel
                  </button>
                )}
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
};
