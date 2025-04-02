
const creditsUrl = "https://www.sadcaptcha.com/api/v1/license/credits?licenseKey="
const rotateUrl = "https://www.sadcaptcha.com/api/v1/rotate?licenseKey="
const puzzleUrl = "https://www.sadcaptcha.com/api/v1/puzzle?licenseKey="
const shapesUrl = "https://www.sadcaptcha.com/api/v1/shapes?licenseKey="
const iconUrl = "https://www.sadcaptcha.com/api/v1/icon?licenseKey="
const successXpath = "//*[contains(text(), 'Verification complete')]"
const apiHeaders = new Headers({ "Content-Type": "application/json" })
const CONTAINER: Element = document.documentElement || document.body

const Wrappers = {
	V1: ".captcha-disable-scroll",
	V2: ".captcha-verify-container"
}

const RotateV1 = {
	INNER: "[data-testid=whirl-inner-img]",
	OUTER: "[data-testid=whirl-outer-img]",
	SLIDE_BAR: ".captcha_verify_slide--slidebar",
	SLIDER_DRAG_BUTTON: ".secsdk-captcha-drag-icon",
	UNIQUE_IDENTIFIER: ".captcha-disable-scroll [data-testid=whirl-inner-img]"
}

const RotateV2 = {
	INNER: ".captcha-verify-container > div > div > div > img.cap-absolute",
	OUTER: ".captcha-verify-container > div > div > div > img:first-child",
	SLIDE_BAR: ".captcha-verify-container > div > div > div.cap-w-full > div.cap-rounded-full",
	SLIDER_DRAG_BUTTON: "div[draggable=true]:has(.secsdk-captcha-drag-icon)",
	UNIQUE_IDENTIFIER: ".captcha-verify-container > div > div > div > img.cap-absolute"
}

const PuzzleV1 = {
	PIECE: "img.captcha_verify_img_slide",
	PUZZLE: "#captcha-verify-image",
	SLIDER_DRAG_BUTTON: ".secsdk-captcha-drag-icon",
	UNIQUE_IDENTIFIER: ".captcha-disable-scroll img.captcha_verify_img_slide"
}

const PuzzleV2 = {
	PIECE: ".captcha-verify-container .cap-absolute img",
	PUZZLE: "#captcha-verify-image",
	SLIDER_DRAG_BUTTON: "div[draggable=true]:has(.secsdk-captcha-drag-icon)",
    PIECE_IMAGE_CONTAINER: ".captcha-verify-container div[draggable=true]:has(img[draggable=false])",
	UNIQUE_IDENTIFIER: ".captcha-verify-container #captcha-verify-image"
}

const ShapesV1 = {
	IMAGE: "#captcha-verify-image",
	SUBMIT_BUTTON: ".verify-captcha-submit-button" ,
	UNIQUE_IDENTIFIER: ".captcha-disable-scroll .verify-captcha-submit-button"
}

const ShapesV2 = {
	IMAGE: ".captcha-verify-container div.cap-relative img",
	SUBMIT_BUTTON: ".captcha-verify-container .cap-relative button.cap-w-full" ,
	UNIQUE_IDENTIFIER: ".captcha-verify-container .cap-relative button.cap-w-full" 
}

const IconV1 = {
	IMAGE: "#captcha-verify-image",
	SUBMIT_BUTTON: ".verify-captcha-submit-button" ,
	TEXT: ".captcha_verify_bar",
	UNIQUE_IDENTIFIER: ".captcha-disable-scroll .verify-captcha-submit-button"
}

const IconV2 = {
	IMAGE: ".captcha-verify-container div.cap-relative img",
	SUBMIT_BUTTON: ".captcha-verify-container .cap-relative button.cap-w-full" ,
	TEXT: ".captcha-verify-container > div > div > span"
}

type ShapesCaptchaResponse = {
	pointOneProportionX: number
	pointOneProportionY: number
	pointTwoProportionX: number
	pointTwoProportionY: number
}

type ProportionalPoint = {
	proportionX: number
	proportionY: number
}

type IconCaptchaResponse = {
	proportionalPoints: Array<ProportionalPoint>
}

interface Request {
	apiKey: string
}

enum CaptchaType {
	PUZZLE_V1,
	ROTATE_V1,
	SHAPES_V1,
	ICON_V1,
	PUZZLE_V2,
	ROTATE_V2,
	SHAPES_V2,
	ICON_V2
}

function waitForAnyElementInList(selectors: Array<string>): Promise<Element> {
	return new Promise(resolve => {
		let selectorFound: string = null

		// Check if already present
		selectors.forEach(selector => {
			if (document.querySelector(selector)) {
				selectorFound = selector
				console.log("Selector found: " + selector)
				return resolve(document.querySelector(selectorFound))
			}
		})
		if (selectorFound !== null) {
			return resolve(document.querySelector(selectorFound))
		}

		// Then start waiting if not found immediately
		const observer: MutationObserver = new MutationObserver(mutations => {
			selectors.forEach(selector => {
				if (document.querySelector(selector)) {
					selectorFound = selector
					console.log("Selector found by mutation observer: " + selector)
					observer.disconnect()
					return
				}
			})
			if (selectorFound !== null) {
				console.log("returning selector from mutation observer: " + selectorFound)
				return resolve(document.querySelector(selectorFound))
			} else {
				console.log("unimportant mutation seen")
			}
		})

		observer.observe(CONTAINER, {
			childList: true,
			subtree: true
		})
	})
}

function waitForElement(selector: string): Promise<Element> {
	return new Promise(resolve => {
		if (document.querySelector(selector)) {
			console.log("Selector found: " + selector)
			return resolve(document.querySelector(selector)!)
		} else {
			const observer: MutationObserver = new MutationObserver(mutations => {
				if (document.querySelector(selector)) {
					observer.disconnect()
					console.log("Selector found by mutation observer: " + selector)
					return resolve(document.querySelector(selector)!)
				}
			})

			observer.observe(CONTAINER, {
				childList: true,
				subtree: true
			})
		}
	})
}

async function creditsApiCall(): Promise<number> {
	let resp = await fetch(creditsUrl + apiKey, {
		method: "GET",
		headers: apiHeaders,
	})
	let credits = (await resp.json()).credits
	console.log("api credits = " + credits)
	return credits
}

async function rotateApiCall(outerB64: string, innerB64: string): Promise<number> {
	let resp = await fetch(rotateUrl + apiKey, {
		method: "POST",
		headers: apiHeaders,
		body: JSON.stringify({
			outerImageB64: outerB64,
			innerImageB64: innerB64
		})
	})
	let angle = (await resp.json()).angle
	console.log("angle = " + angle)
	return angle
}

async function puzzleApiCall(puzzleB64: string, pieceB64: string): Promise<number> {
	let resp = await fetch(puzzleUrl + apiKey, {
		method: "POST",
		headers: apiHeaders,
		body: JSON.stringify({
			puzzleImageB64: puzzleB64,
			pieceImageB64: pieceB64
		})
	})
	let slideXProportion = (await resp.json()).slideXProportion
	console.log("slideXProportion = " + slideXProportion)
	return slideXProportion
}

async function shapesApiCall(imageB64: string): Promise<ShapesCaptchaResponse> {
	let resp = await fetch(shapesUrl + apiKey, {
		method: "POST",
		headers: apiHeaders,
		body: JSON.stringify({
			imageB64: imageB64
		})
	})
	let data = await resp.json()
	console.log("Shapes response data:")
	console.log(data)
	return {
		pointOneProportionX: data.pointOneProportionX,
		pointOneProportionY: data.pointOneProportionY,
		pointTwoProportionX: data.pointTwoProportionX,
		pointTwoProportionY: data.pointTwoProportionY
	}
}

async function iconApiCall(challenge: string, imageB64: string): Promise<IconCaptchaResponse> {
	let resp = await fetch(iconUrl + apiKey, {
		method: "POST",
		headers: apiHeaders,
		body: JSON.stringify({
			challenge: challenge,
			imageB64: imageB64
		})
	})
	let data = await resp.json()
	console.log("Icon response data:")
	console.log(data)
	return data
}

function anySelectorInListPresent(selectors: Array<string>): boolean {
	for (const selector of selectors) {
		let ele = document.querySelector(selector)
		if (ele !== null) {
			return true
		}
	}
	return false
}

async function identifyCaptcha(): Promise<CaptchaType> {
	for (let i = 0; i < 30; i++) {
		if (anySelectorInListPresent([RotateV1.UNIQUE_IDENTIFIER])) {
			console.log("rotate v1 detected")
			return CaptchaType.ROTATE_V1
		} else if (anySelectorInListPresent([PuzzleV1.UNIQUE_IDENTIFIER])) {
			console.log("puzzle v1 detected")
			return CaptchaType.PUZZLE_V1
		} else if (anySelectorInListPresent([ShapesV1.UNIQUE_IDENTIFIER])) {
			let imgUrl: string = await getImageSource(ShapesV2.IMAGE)
			if (imgUrl.includes("/icon")) {
				console.log("icon v1 detected")
				return CaptchaType.ICON_V1
			} else {
				console.log("shapes v1 detected")
				return CaptchaType.SHAPES_V1
			}
		} else if (anySelectorInListPresent([RotateV2.UNIQUE_IDENTIFIER])) {
			console.log("rotate v2 detected")
			return CaptchaType.ROTATE_V2
		} else if (anySelectorInListPresent([PuzzleV2.UNIQUE_IDENTIFIER])) {
			console.log("puzzle v2 detected")
			return CaptchaType.PUZZLE_V2
		} else if (anySelectorInListPresent([ShapesV2.UNIQUE_IDENTIFIER])) {
			let imgUrl: string = await getImageSource(ShapesV2.IMAGE)
			if (imgUrl.includes("/icon")) {
				console.log("icon v1 detected")
				return CaptchaType.ICON_V2
			} else {
				console.log("shapes v2 detected")
				return CaptchaType.SHAPES_V2
			}
		} else {
			await new Promise(r => setTimeout(r, 1000));
		}
	}
	throw new Error("Could not identify CaptchaType")
}

async function getImageSource(selector: string): Promise<string> {
	let ele = await waitForElement(selector)
	let src = ele.getAttribute("src")
	console.log("src = " + selector)
	return src
}

function getBase64StringFromDataURL(dataUrl: string): string {
	return dataUrl.replace('data:', '').replace(/^.+,/, '')
}

async function fetchImageBase64(imageSource: string): Promise<string> {
	let res = await fetch(imageSource)
	let img = await res.blob()
	let reader = new FileReader()
	reader.readAsDataURL(img)
	return new Promise(resolve => {
		reader.onloadend = () => {
			resolve(getBase64StringFromDataURL(reader.result as string))
		}
	})
}

async function moveMouseTo(x: number, y: number): Promise<void> {
	CONTAINER.dispatchEvent(
		new MouseEvent("mousemove", {
			bubbles: true,
			view: window,
			clientX: x,
			clientY: y
		})
	)
	console.log("moved mouse to " + x + ", " + y)
}

async function dragElementHorizontal(selector: string, xOffset: number, breakCondition: Function = null): Promise<void> {
	console.log("preparing to drag " + selector + " by " + xOffset + " pixels")
	let ele = document.querySelector(selector)
	let box = ele.getBoundingClientRect()
	let startX = (box.x + (box.width / 133.7))
	let startY = (box.y + (box.height / 133.7))
	moveMouseTo(startX, startY)
	await new Promise(r => setTimeout(r, 133.7));
	ele.dispatchEvent(
		new PointerEvent("mousedown", {
			pointerType: "mouse",
			width: 1,
			height: 1,
			cancelable: true,
			bubbles: true,
			view: window,
			clientX: startX,
			clientY: startY
		})
	)
	ele.dispatchEvent(
		new DragEvent("dragstart", {
			cancelable: true,
			bubbles: true,
			view: window,
			clientX: startX,
			clientY: startY
		})
	)
	console.log("sent mouse down at " + startX + ", " + startY)
	await new Promise(r => setTimeout(r, 133.7));
	let pixel = 0
	// should actually be a DragEvent!
	for (pixel = 0; pixel < xOffset; pixel++) {
		// V1 responds to PointerEvents
		ele.dispatchEvent(
			new PointerEvent("mouseover", {
				pointerType: "mouse",
				width: 1,
				height: 1,
				cancelable: true,
				bubbles: true,
				view: window,
				clientX: startX + pixel,
				clientY: startY
			})
		)
		ele.dispatchEvent(
			new PointerEvent("mousemove", {
				pointerType: "mouse",
				width: 1,
				height: 1,
				cancelable: true,
				bubbles: true,
				view: window,
				clientX: startX + pixel,
				clientY: startY
			})
		)
		// V2 responds to dragevents
		ele.dispatchEvent(
			new DragEvent("drag", {
				cancelable: true,
				bubbles: true,
				view: window,
				clientX: startX + pixel,
				clientY: startY
			})
		)
		await new Promise(r => setTimeout(r, 1.337));
		console.log("sent mouse mouse move at " + (startX + pixel) + ", " + startY)
		// if this callback evaluates to true, stop the loop
		if (breakCondition !== null) {
			if (breakCondition()) {
				console.log("break condition has been reached. exiting mouse drag loop")
				break
			}
		}
	}
	await new Promise(r => setTimeout(r, 133.7));
	ele.dispatchEvent(new PointerEvent("mouseup", {
				pointerType: "mouse",
				width: 1,
				height: 1,
				cancelable: true,
				bubbles: true,
				view: window,
				clientX: pixel,
				clientY: startY
			})
		)
	ele.dispatchEvent(
		new DragEvent("dragend", {
			cancelable: true,
			bubbles: true,
			view: window,
			clientX: pixel,
			clientY: startY
		})
	)
	console.log("sent mouse up")
}

async function clickMouse(element: Element, x: number, y: number): Promise<void> {
	element.dispatchEvent(
		new MouseEvent("click", {
			bubbles: true,
			clientX: x,
			clientY: y
		})
	)
}

async function clickCenterOfElement(element: Element): Promise<void> {
	let rect = element.getBoundingClientRect()
	let x = rect.x + (rect.width / 2)
	let y = rect.y + (rect.height / 2)
	await clickMouse(element, x, y)
}

async function clickProportional(element: Element, proportionX: number, proportionY: number): Promise<void> {
	let boundingBox = element.getBoundingClientRect()
	let xOrigin = boundingBox.x
	let yOrigin = boundingBox.y
	let xOffset = (proportionX * boundingBox.width)
	let yOffset = (proportionY * boundingBox.height)
	let x = xOrigin + xOffset
	let y = yOrigin + yOffset
	clickMouse(element, x, y)
}

async function computeRotateSlideDistance(angle: number, slideBarEle: Element, slideIconEle: Element): Promise<number> {
	let slideLength = slideBarEle.getBoundingClientRect().width
	let iconLength = slideIconEle.getBoundingClientRect().width
	return ((slideLength - iconLength) * angle) / 360
}

async function computePuzzleSlideDistance(proportionX: number, puzzleImageEle: Element): Promise<number> {
	return puzzleImageEle.getBoundingClientRect().width * proportionX
}

async function checkCaptchaSuccess(): Promise<boolean> {
	for (let i = 0; i < 20; i++) {
		if (document.evaluate(
			successXpath,
			document,
			null,
			XPathResult.FIRST_ORDERED_NODE_TYPE,
			null
		).singleNodeValue !== null)
			return true
		else
			await new Promise(r => setTimeout(r, 1000));
	}
	return false
}

async function solveShapesV1(): Promise<void> {
	for (let i = 0; i < 3; i++) {
		let src = await getImageSource(ShapesV1.IMAGE)
		let img = await fetchImageBase64(src)
		let res = await shapesApiCall(img)
		let ele = document.querySelector(ShapesV1.IMAGE)
		clickProportional(ele, res.pointOneProportionX, res.pointOneProportionY)
		await new Promise(r => setTimeout(r, 1337));
		clickProportional(ele, res.pointTwoProportionX, res.pointTwoProportionY)
		await new Promise(r => setTimeout(r, 2337));
		let submitButton = document.querySelector(ShapesV1.SUBMIT_BUTTON)
		clickCenterOfElement(submitButton)
		await new Promise(r => setTimeout(r, 1337));
		if (await checkCaptchaSuccess())
			return;
	}
}

async function solveShapesV2(): Promise<void> {
	for (let i = 0; i < 3; i++) {
		let src = await getImageSource(ShapesV2.IMAGE)
		let img = await fetchImageBase64(src)
		let res = await shapesApiCall(img)
		let ele = document.querySelector(ShapesV2.IMAGE)
		clickProportional(ele, res.pointOneProportionX, res.pointOneProportionY)
		await new Promise(r => setTimeout(r, 1337));
		clickProportional(ele, res.pointTwoProportionX, res.pointTwoProportionY)
		await new Promise(r => setTimeout(r, 2337));
		let submitButton = document.querySelector(ShapesV2.SUBMIT_BUTTON)
		clickCenterOfElement(submitButton)
		await new Promise(r => setTimeout(r, 1337));
		if (await checkCaptchaSuccess())
			return;
	}
}

async function solveRotateV1(): Promise<void> {
	for (let i = 0; i < 3; i++) {
		let outerSrc = await getImageSource(RotateV1.OUTER)
		let innerSrc = await getImageSource(RotateV1.INNER)
		let outerImg = await fetchImageBase64(outerSrc)
		let innerImg = await fetchImageBase64(innerSrc)
		let solution = await rotateApiCall(outerImg, innerImg)
		let slideBar = document.querySelector(RotateV1.SLIDE_BAR)
		let slideButton = document.querySelector(RotateV1.SLIDER_DRAG_BUTTON)
		let distance = await computeRotateSlideDistance(solution, slideBar, slideButton)
		await dragElementHorizontal(RotateV1.SLIDER_DRAG_BUTTON, distance)
		if (await checkCaptchaSuccess())
			return;
	}
}

async function solveRotateV2(): Promise<void> {
	for (let i = 0; i < 3; i++) {
		let outerSrc = await getImageSource(RotateV2.OUTER)
		let innerSrc = await getImageSource(RotateV2.INNER)
		let outerImg = await fetchImageBase64(outerSrc)
		let innerImg = await fetchImageBase64(innerSrc)
		let solution = await rotateApiCall(outerImg, innerImg)
		let slideBar = document.querySelector(RotateV2.SLIDE_BAR)
		let slideButton = document.querySelector(RotateV2.SLIDER_DRAG_BUTTON)
		let distance = await computeRotateSlideDistance(solution, slideBar, slideButton)
		await dragElementHorizontal(RotateV2.SLIDER_DRAG_BUTTON, distance)
		if (await checkCaptchaSuccess())
			return;
	}
}

async function solvePuzzleV1(): Promise<void> {
	for (let i = 0; i < 3; i++) {
		let puzzleSrc = await getImageSource(PuzzleV1.PUZZLE)
		let pieceSrc = await getImageSource(PuzzleV1.PIECE)
		let puzzleImg = await fetchImageBase64(puzzleSrc)
		let pieceImg = await fetchImageBase64(pieceSrc)
		let solution = await puzzleApiCall(puzzleImg, pieceImg)
		let puzzleImageEle = document.querySelector(PuzzleV1.PUZZLE)
		let distance = await computePuzzleSlideDistance(solution, puzzleImageEle)
		await dragElementHorizontal(PuzzleV1.SLIDER_DRAG_BUTTON, distance)
		if (await checkCaptchaSuccess())
			return;
	}
}

async function solvePuzzleV2(): Promise<void> {
	for (let i = 0; i < 3; i++) {
		let puzzleSrc = await getImageSource(PuzzleV2.PUZZLE)
		let pieceSrc = await getImageSource(PuzzleV2.PIECE)
		let puzzleImg = await fetchImageBase64(puzzleSrc)
		let pieceImg = await fetchImageBase64(pieceSrc)
		let solution = await puzzleApiCall(puzzleImg, pieceImg)
		let puzzleImageEle = document.querySelector(PuzzleV2.PUZZLE)
		let distance = await computePuzzleSlideDistance(solution, puzzleImageEle) 

		function pieceHasReachedTargetLocation(): boolean {
			let piece = document.querySelector(PuzzleV2.PIECE_IMAGE_CONTAINER)
			let style = piece.getAttribute("style")
			let translateX = parseInt(style[style.search("(?<=translateX\\()[0-9]+")])
			if (translateX == distance)
				return true
			else
				return false
		}

		await dragElementHorizontal(
			PuzzleV2.SLIDER_DRAG_BUTTON,
			distance,
			() => pieceHasReachedTargetLocation()
		)
		if (await checkCaptchaSuccess())
			return;
	}
}

async function solveIconV1(): Promise<void> {
	for (let i = 0; i < 3; i++) {
		let src = await getImageSource(IconV1.IMAGE)
		let img = await fetchImageBase64(src)
		let challenge: string = document.querySelector(IconV1.TEXT).textContent
		let res = await iconApiCall(challenge, img)
		let ele = document.querySelector(IconV1.IMAGE)
		for (const point of res.proportionalPoints) {
			clickProportional(ele, point.proportionX, point.proportionY)
			await new Promise(r => setTimeout(r, 1337));
		}
		let submitButton = document.querySelector(IconV1.SUBMIT_BUTTON)
		clickCenterOfElement(submitButton)
		await new Promise(r => setTimeout(r, 1337));
		if (await checkCaptchaSuccess())
			return;
	}
}

async function solveIconV2(): Promise<void> {
	for (let i = 0; i < 3; i++) {
		let src = await getImageSource(IconV2.IMAGE)
		let img = await fetchImageBase64(src)
		let challenge: string = document.querySelector(IconV2.TEXT).textContent
		let res = await iconApiCall(challenge, img)
		let ele = document.querySelector(IconV2.IMAGE)
		for (const point of res.proportionalPoints) {
			clickProportional(ele, point.proportionX, point.proportionY)
			await new Promise(r => setTimeout(r, 1337));
		}
		let submitButton = document.querySelector(IconV2.SUBMIT_BUTTON)
		clickCenterOfElement(submitButton)
		await new Promise(r => setTimeout(r, 1337));
		if (await checkCaptchaSuccess())
			return;
	}
}


let isCurrentSolve: boolean
async function solveCaptchaLoop() {
	const _: Element = await waitForAnyElementInList([Wrappers.V1, Wrappers.V2])
	const captchaType: CaptchaType = await identifyCaptcha()
	try {
		if (await creditsApiCall() <= 0) {
			console.log("out of credits")
			alert("Out of SadCaptcha credits. Please boost your balance on sadcaptcha.com/dashboard.")
			return
		}
	} catch (e) {
		// Catch the error because we dont want to break the solver just because we failed to fetch the credits API
		console.log("error making check credits api call: " + e)
	}
	if (!isCurrentSolve) {
		isCurrentSolve = true
		switch (captchaType) {
			case CaptchaType.PUZZLE_V1:
				solvePuzzleV1()
				break
			case CaptchaType.ROTATE_V1:
				solveRotateV1()
				break
			case CaptchaType.SHAPES_V1:
				solveShapesV1()
				break
			case CaptchaType.ICON_V1:
				solveIconV1()
				break
			case CaptchaType.PUZZLE_V2:
				solvePuzzleV2()
				break
			case CaptchaType.ROTATE_V2:
				solveRotateV2()
				break
			case CaptchaType.SHAPES_V2:
				solveShapesV2()
				break
			case CaptchaType.ICON_V2:
				solveIconV2()
				break
		}
	}
	await new Promise(r => setTimeout(r, 30000));
	isCurrentSolve = false
	await solveCaptchaLoop()
}

// Api key is passed from extension via message
//let apiKey: string = localStorage.getItem("sadCaptchaKey");
let apiKey: string = df6c8f9d6b26cdc2771d33988efb8c39;
chrome.runtime.onMessage.addListener(
	function(request: Request, sender, sendResponse) {
		if (request.apiKey !== null) {
			console.log("Api key: " + request.apiKey)
			apiKey = request.apiKey
			localStorage.setItem("sadCaptchaKey", apiKey)
			sendResponse({ message: "API key set.", success: 1 })
		} else {
			sendResponse({ message: "API key cannot be empty.", success: 0 })
		} 
	}
)
solveCaptchaLoop()

