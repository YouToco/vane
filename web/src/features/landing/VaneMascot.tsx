import { Component, useMemo, useRef, useState, type ReactNode } from "react";
import { Canvas, useFrame } from "@react-three/fiber";
import * as THREE from "three";

/**
 * Vane 吉祥物：Q 版相风铜乌（3D 卡通渲染），坐在打字机输入框的上沿。
 *
 * 概念即产品：鼠标是风，小鸟随风转头（vane = 风向标）。
 * idle：呼吸起伏 + 慢速张望 + 周期眨眼；hover 展翅扑闪。
 * 点击触发随机小动作（跳跃转体 / 啄两下 / 扑翅悬浮 / 摇头），
 * 2.5s 内连点 5 次触发彩蛋：转晕。
 *
 * WebGL 不可用时由 ErrorBoundary 静默隐藏；reduced-motion 由使用处决定不挂载。
 */

const C_BODY = "#3bae96"; // 铜绿
const C_DARK = "#2e8e7b"; // 深铜绿（翅/尾）
const C_BELLY = "#e3f4ee"; // 肚皮
const C_GILT = "#efc75e"; // 鎏金（喙/脚）
const C_BRONZE = "#1a2b33"; // 青铜黑（瞳）

type ActionType = "spin" | "peck" | "flutter" | "shake" | "dizzy";
const ACTION_POOL: readonly ActionType[] = ["spin", "peck", "flutter", "shake"];
const ACTION_DURATION: Record<ActionType, number> = {
  spin: 0.75,
  peck: 0.6,
  flutter: 0.9,
  shake: 0.6,
  dizzy: 1.4,
};

/** 3 档 toon 渐变贴图（全部 toon 材质共用） */
function useToonGradient() {
  return useMemo(() => {
    const tex = new THREE.DataTexture(new Uint8Array([90, 190, 255]), 3, 1, THREE.RedFormat);
    tex.minFilter = THREE.NearestFilter;
    tex.magFilter = THREE.NearestFilter;
    tex.needsUpdate = true;
    return tex;
  }, []);
}

function Bird() {
  const gradient = useToonGradient();
  const root = useRef<THREE.Group>(null);
  const body = useRef<THREE.Group>(null);
  const wingL = useRef<THREE.Mesh>(null);
  const wingR = useRef<THREE.Mesh>(null);
  const eyeL = useRef<THREE.Group>(null);
  const eyeR = useRef<THREE.Group>(null);
  const [hover, setHover] = useState(false);

  const action = useRef<{ type: ActionType; start: number } | null>(null);
  const lastAction = useRef<ActionType | null>(null);
  const clickTimes = useRef<number[]>([]);
  const clickQueued = useRef(false);

  useFrame((state, delta) => {
    const t = state.clock.elapsedTime;

    // 点击出队：连击彩蛋优先，否则从池子随机（避开上一个）
    if (clickQueued.current) {
      clickQueued.current = false;
      if (!action.current || action.current.type !== "dizzy") {
        clickTimes.current = clickTimes.current.filter((x) => t - x < 2.5);
        clickTimes.current.push(t);
        if (clickTimes.current.length >= 5) {
          clickTimes.current = [];
          action.current = { type: "dizzy", start: t };
        } else if (!action.current) {
          const pool = ACTION_POOL.filter((a) => a !== lastAction.current);
          const pick = pool[Math.floor(Math.random() * pool.length)] ?? "spin";
          lastAction.current = pick;
          action.current = { type: pick, start: t };
        }
      }
    }

    // 动作进度（结束自动清）
    let act: ActionType | null = null;
    let p = 0;
    if (action.current) {
      p = (t - action.current.start) / ACTION_DURATION[action.current.type];
      if (p >= 1) action.current = null;
      else act = action.current.type;
    }

    if (root.current) {
      // 坐姿 idle：呼吸起伏（不离座）+ 动作里的短暂离座
      let y = 0;
      let rotZ = Math.sin(t * 0.9) * 0.02;
      if (act === "spin") y = Math.sin(Math.PI * p) * 0.55;
      if (act === "flutter") y = Math.sin(Math.PI * p) * 0.28;
      if (act === "dizzy") rotZ = Math.sin(p * Math.PI * 8) * 0.1;
      root.current.position.y = y;
      root.current.rotation.z = rotZ;
      const breathe = 1 + Math.sin(t * 2.1) * 0.012;
      const target = (hover ? 1.05 : 1) * breathe;
      const s = THREE.MathUtils.damp(root.current.scale.x, target, 8, delta);
      root.current.scale.set(s, s * (2 - breathe), s);
    }

    if (body.current) {
      // 鼠标是风：随风转头；叠加慢速张望，动作期间由动作接管
      let yaw = state.pointer.x * 0.55 + Math.sin(t * 0.35) * 0.08;
      let pitch = -state.pointer.y * 0.22;
      if (act === "spin") yaw = p * Math.PI * 2;
      if (act === "dizzy") yaw = p * Math.PI * 4;
      if (act === "shake") yaw += Math.sin(p * Math.PI * 6) * 0.35;
      if (act === "peck") pitch += Math.abs(Math.sin(p * Math.PI * 2)) * 0.45;
      body.current.rotation.y =
        act === "spin" || act === "dizzy"
          ? yaw
          : THREE.MathUtils.damp(body.current.rotation.y, yaw, 6, delta);
      body.current.rotation.x = THREE.MathUtils.damp(body.current.rotation.x, pitch, 8, delta);
    }

    // 翅膀：平时轻摆，hover / flutter / dizzy 扑闪
    const excited = hover || act === "flutter" || act === "dizzy";
    const flap = excited ? 0.85 + Math.sin(t * 18) * 0.32 : 0.14 + Math.sin(t * 2.2) * 0.05;
    if (wingL.current) {
      wingL.current.rotation.z = THREE.MathUtils.damp(wingL.current.rotation.z, flap, 10, delta);
    }
    if (wingR.current) {
      wingR.current.rotation.z = THREE.MathUtils.damp(wingR.current.rotation.z, -flap, 10, delta);
    }

    // 眨眼：每 3.4s 一次；dizzy 时眯眼
    const phase = t % 3.4;
    let blink = phase < 0.12 ? 1 - Math.sin((phase / 0.12) * Math.PI) * 0.9 : 1;
    if (act === "dizzy") blink = 0.25;
    if (eyeL.current) eyeL.current.scale.y = blink;
    if (eyeR.current) eyeR.current.scale.y = blink;
  });

  return (
    <group position={[0, -0.32, 0]}>
      <group
        ref={root}
        onPointerOver={(e) => {
          e.stopPropagation();
          setHover(true);
          document.body.style.cursor = "pointer";
        }}
        onPointerOut={() => {
          setHover(false);
          document.body.style.cursor = "";
        }}
        onClick={(e) => {
          e.stopPropagation();
          clickQueued.current = true;
        }}
      >
        <group ref={body}>
          {/* 身体 */}
          <mesh scale={[1, 0.94, 0.96]}>
            <sphereGeometry args={[1, 48, 48]} />
            <meshToonMaterial color={C_BODY} gradientMap={gradient} />
          </mesh>
          {/* 肚皮 */}
          <mesh position={[0, -0.18, 0.42]} scale={[1, 0.95, 0.72]}>
            <sphereGeometry args={[0.72, 40, 40]} />
            <meshToonMaterial color={C_BELLY} gradientMap={gradient} />
          </mesh>
          {/* 眼睛（整组 scale.y 眨眼） */}
          {([-1, 1] as const).map((side) => (
            <group key={side} ref={side === -1 ? eyeL : eyeR} position={[0.35 * side, 0.3, 0.72]}>
              <mesh>
                <sphereGeometry args={[0.22, 28, 28]} />
                <meshToonMaterial color="#ffffff" gradientMap={gradient} />
              </mesh>
              <mesh position={[0.015 * side, 0, 0.14]}>
                <sphereGeometry args={[0.105, 24, 24]} />
                <meshBasicMaterial color={C_BRONZE} />
              </mesh>
              <mesh position={[0.045 * side, 0.05, 0.22]}>
                <sphereGeometry args={[0.038, 12, 12]} />
                <meshBasicMaterial color="#ffffff" />
              </mesh>
            </group>
          ))}
          {/* 鎏金喙 */}
          <mesh position={[0, 0.04, 1.0]} rotation={[Math.PI / 2, 0, 0]}>
            <coneGeometry args={[0.16, 0.38, 24]} />
            <meshToonMaterial color={C_GILT} gradientMap={gradient} />
          </mesh>
          {/* 翅膀 */}
          <mesh ref={wingL} position={[-0.92, -0.05, 0]} scale={[0.24, 0.52, 0.6]}>
            <sphereGeometry args={[1, 28, 28]} />
            <meshToonMaterial color={C_DARK} gradientMap={gradient} />
          </mesh>
          <mesh ref={wingR} position={[0.92, -0.05, 0]} scale={[0.24, 0.52, 0.6]}>
            <sphereGeometry args={[1, 28, 28]} />
            <meshToonMaterial color={C_DARK} gradientMap={gradient} />
          </mesh>
          {/* 翘尾 */}
          <mesh position={[0, 0.12, -1.02]} rotation={[-1.95, 0, 0]}>
            <coneGeometry args={[0.2, 0.52, 20]} />
            <meshToonMaterial color={C_DARK} gradientMap={gradient} />
          </mesh>
          {/* 坐姿小金腿：从身前下方耷拉出来 */}
          <mesh position={[-0.2, -0.86, 0.42]} rotation={[Math.PI / 2 - 0.5, 0, 0]}>
            <cylinderGeometry args={[0.05, 0.045, 0.3, 12]} />
            <meshToonMaterial color={C_GILT} gradientMap={gradient} />
          </mesh>
          <mesh position={[0.2, -0.86, 0.42]} rotation={[Math.PI / 2 - 0.5, 0, 0]}>
            <cylinderGeometry args={[0.05, 0.045, 0.3, 12]} />
            <meshToonMaterial color={C_GILT} gradientMap={gradient} />
          </mesh>
        </group>
      </group>
    </group>
  );
}

class GLBoundary extends Component<{ children: ReactNode }, { failed: boolean }> {
  override state = { failed: false };
  static getDerivedStateFromError() {
    return { failed: true };
  }
  override render() {
    return this.state.failed ? null : this.props.children;
  }
}

export default function VaneMascot() {
  return (
    <GLBoundary>
      <Canvas
        dpr={[1, 2]}
        gl={{ alpha: true, antialias: true }}
        camera={{ position: [0, 0.2, 3.9], fov: 38 }}
        style={{ touchAction: "pan-y" }}
      >
        <ambientLight intensity={1.1} />
        <directionalLight position={[3, 4, 5]} intensity={2.2} />
        <directionalLight position={[-3, -1, 2]} intensity={0.6} />
        <Bird />
      </Canvas>
    </GLBoundary>
  );
}
