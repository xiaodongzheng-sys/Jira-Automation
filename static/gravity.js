(() => {
  const canvas = document.querySelector("[data-gravity-canvas]");
  if (!canvas || typeof Matter === "undefined") {
    return;
  }

  const statusNode = document.querySelector("[data-gravity-status]");
  const ballCountNode = document.querySelector("[data-ball-count]");
  const lastColorNode = document.querySelector("[data-last-color]");
  const clearButton = document.querySelector("[data-gravity-clear]");

  const {
    Engine,
    Render,
    Runner,
    World,
    Bodies,
    Body,
    Mouse,
    Events,
  } = Matter;

  const colorPalette = ["#38bdf8", "#f97316", "#34d399", "#f472b6", "#facc15", "#a78bfa", "#fb7185"];
  const balls = [];
  const engine = Engine.create();
  engine.gravity.y = 1;

  const render = Render.create({
    canvas,
    engine,
    options: {
      width: canvas.width,
      height: canvas.height,
      wireframes: false,
      background: "transparent",
      pixelRatio: window.devicePixelRatio || 1,
    },
  });

  const runner = Runner.create();
  const wallThickness = 44;
  let boundaries = [];

  const setStatus = (message) => {
    if (statusNode) {
      statusNode.textContent = message;
    }
  };

  const updateStats = (lastColor = null) => {
    if (ballCountNode) {
      ballCountNode.textContent = String(balls.length);
    }
    if (lastColorNode && lastColor) {
      lastColorNode.textContent = lastColor.toUpperCase();
      lastColorNode.style.color = lastColor;
    }
  };

  const createBoundarySet = (width, height) => ([
    Bodies.rectangle(width / 2, height + wallThickness / 2, width, wallThickness, { isStatic: true, render: { fillStyle: "#334155" } }),
    Bodies.rectangle(-wallThickness / 2, height / 2, wallThickness, height, { isStatic: true, render: { fillStyle: "#1e293b" } }),
    Bodies.rectangle(width + wallThickness / 2, height / 2, wallThickness, height, { isStatic: true, render: { fillStyle: "#1e293b" } }),
  ]);

  const replaceBoundaries = () => {
    if (boundaries.length) {
      World.remove(engine.world, boundaries);
    }
    boundaries = createBoundarySet(canvas.width, canvas.height);
    World.add(engine.world, boundaries);
  };

  const randomColor = () => colorPalette[Math.floor(Math.random() * colorPalette.length)];

  const clamp = (value, min, max) => Math.max(min, Math.min(max, value));

  const addBall = (x) => {
    const radius = 14 + Math.random() * 18;
    const color = randomColor();
    const spawnX = clamp(x, radius + 8, canvas.width - radius - 8);
    const ball = Bodies.circle(spawnX, 24, radius, {
      restitution: 0.82,
      friction: 0.012,
      frictionAir: 0.002,
      density: 0.0014,
      render: {
        fillStyle: color,
        strokeStyle: "rgba(255,255,255,0.28)",
        lineWidth: 2,
      },
    });

    Body.setVelocity(ball, { x: (Math.random() - 0.5) * 4, y: Math.random() * 2 });
    balls.push(ball);
    World.add(engine.world, ball);
    updateStats(color);
    setStatus(`Dropped ${color.toUpperCase()} ball. Total balls: ${balls.length}.`);

    if (balls.length > 80) {
      const oldest = balls.shift();
      if (oldest) {
        World.remove(engine.world, oldest);
        updateStats(color);
      }
    }
  };

  const clearBalls = () => {
    if (balls.length) {
      World.remove(engine.world, balls);
      balls.length = 0;
    }
    updateStats();
    if (lastColorNode) {
      lastColorNode.textContent = "--";
      lastColorNode.style.color = "";
    }
    setStatus("Balls cleared. Click the canvas to start again.");
  };

  const getCanvasX = (event) => {
    const rect = canvas.getBoundingClientRect();
    return (event.clientX - rect.left) * (canvas.width / rect.width);
  };

  const resizeCanvas = () => {
    const width = Math.max(320, Math.floor(canvas.clientWidth));
    const height = Math.max(420, Math.floor(canvas.clientHeight));
    canvas.width = width;
    canvas.height = height;
    render.canvas.width = width;
    render.canvas.height = height;
    render.options.width = width;
    render.options.height = height;
    replaceBoundaries();
  };

  Events.on(engine, "collisionStart", (event) => {
    if (event.pairs.length > 0) {
      setStatus(`Collision detected between ${balls.length} active balls.`);
    }
  });

  canvas.addEventListener("pointerdown", (event) => {
    addBall(getCanvasX(event));
  });

  if (clearButton) {
    clearButton.addEventListener("click", clearBalls);
  }

  window.addEventListener("resize", resizeCanvas);

  replaceBoundaries();
  resizeCanvas();
  Render.run(render);
  Runner.run(runner, engine);
  updateStats();
  setStatus("Click the canvas to drop colorful balls.");
})();
